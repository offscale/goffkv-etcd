package goffkv_etcd

import (
    "time"
    "bytes"
    "context"
    goffkv "github.com/offscale/goffkv"
    etcdapi "go.etcd.io/etcd/clientv3"
    etcdapipb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

const (
    leaseTtlSeconds = 10
)

type etcdClient struct {
    client *etcdapi.Client
    prefixSegments []string
    leaseId etcdapi.LeaseID
    leaseKaDoneCh chan interface{}
}

type txnDraft struct {
    checks []etcdapi.Cmp
    onSuccess []etcdapi.Op
    onFailure []etcdapi.Op
}

type keysRange struct {
    left string
    right string
}

func (c *etcdClient) assemblePathNormally(segments []string) string {
    var result bytes.Buffer

    for _, segment := range c.prefixSegments {
        result.WriteByte('/')
        result.WriteString(segment)
    }

    for _, segment := range segments {
        result.WriteByte('/')
        result.WriteString(segment)
    }

    return result.String()
}

func (c *etcdClient) assembleKeyWithNul(segments []string) string {
    nsegments := len(segments)
    if nsegments == 0 {
        panic("key segments can not be empty")
    }
    parent := c.assemblePathNormally(segments[:nsegments - 1])
    return parent + "/\x00" + segments[nsegments - 1]
}

func (c *etcdClient) subtreeRange(segments []string) keysRange {
    s := c.assemblePathNormally(segments)
    return keysRange{
        left:  s + "/",
        right: s + "0", // '0' goes after '/' in ASCII
    }
}

func (c *etcdClient) directChildrenRange(segments []string) keysRange {
    s := c.assemblePathNormally(segments)
    return keysRange{
        left:  s + "/\x00",
        right: s + "/\x01",
    }
}

func New(address string, prefix string) (goffkv.Client, error) {
    prefixSegments, err := goffkv.DisassemblePath(prefix)
    if err != nil {
        return nil, err
    }

    client, err := etcdapi.New(etcdapi.Config{
        Endpoints: []string{address},
    })
    if err != nil {
        return nil, err
    }

    return &etcdClient{
        client: client,
        prefixSegments: prefixSegments,
        leaseId: etcdapi.NoLease,
        leaseKaDoneCh: nil,
    }, nil
}

func (c *etcdClient) checkExists(segments []string) etcdapi.Cmp {
    return etcdapi.Compare(
        etcdapi.CreateRevision(c.assembleKeyWithNul(segments)),
        ">",
        0)
}

func (c *etcdClient) checkNotExists(segments []string) etcdapi.Cmp {
    return etcdapi.Compare(
        etcdapi.CreateRevision(c.assembleKeyWithNul(segments)),
        "=",
        0)
}

func (c *etcdClient) checkLeaseId(segments []string, leaseId etcdapi.LeaseID) etcdapi.Cmp {
    return etcdapi.Compare(
        etcdapi.LeaseValue(c.assembleKeyWithNul(segments)),
        "=",
        leaseId)
}

func (c *etcdClient) checkVersion(segments []string, ver int64) etcdapi.Cmp {
    return etcdapi.Compare(
        etcdapi.Version(c.assembleKeyWithNul(segments)),
        "=",
        ver)
}

func (c *etcdClient) get(segments []string, opts ...etcdapi.OpOption) etcdapi.Op {
    return etcdapi.OpGet(
        c.assembleKeyWithNul(segments),
        opts...)
}

func (c *etcdClient) put(segments []string, value []byte, opts ...etcdapi.OpOption) etcdapi.Op {
    return etcdapi.OpPut(
        c.assembleKeyWithNul(segments),
        string(value),
        opts...)
}

func (c *etcdClient) del(segments []string, opts ...etcdapi.OpOption) etcdapi.Op {
    return etcdapi.OpDelete(
        c.assembleKeyWithNul(segments),
        opts...)
}

func (c *etcdClient) getRange(r keysRange, opts ...etcdapi.OpOption) etcdapi.Op {
    opts = append(opts, etcdapi.WithRange(r.right))
    return etcdapi.OpGet(r.left, opts...)
}

func (c *etcdClient) delRange(r keysRange, opts ...etcdapi.OpOption) etcdapi.Op {
    opts = append(opts, etcdapi.WithRange(r.right))
    return etcdapi.OpDelete(r.left, opts...)
}

func (td *txnDraft) addCheck(c etcdapi.Cmp) {
    td.checks = append(td.checks, c)
}

func (td *txnDraft) doOnSuccess(op etcdapi.Op) {
    td.onSuccess = append(td.onSuccess, op)
}

func (td *txnDraft) doOnFailure(op etcdapi.Op) {
    td.onFailure = append(td.onFailure, op)
}

func (c *etcdClient) addParentExistsCheck(segments []string, td *txnDraft) bool {
    parentSegments := segments[:len(segments) - 1]
    if len(parentSegments) == 0 {
        return false
    }
    td.addCheck(c.checkExists(parentSegments))
    td.doOnFailure(c.get(parentSegments, etcdapi.WithKeysOnly()))
    return true
}

func (c *etcdClient) commitTxnDraft(td txnDraft) (*etcdapi.TxnResponse, error) {
    return c.client.Txn(context.TODO()).
        If(td.checks...).
        Then(td.onSuccess...).
        Else(td.onFailure...).
        Commit()
}

func rrExtractVersion(r *etcdapipb.ResponseOp) uint64 {
    kv := r.GetResponseRange().GetKvs()[0]
    return uint64(kv.Version)
}

func rrEmpty(r *etcdapipb.ResponseOp) bool {
    return r.GetResponseRange().GetCount() == 0
}

func (c *etcdClient) makeWatch(
            header *etcdapipb.ResponseHeader,
            key string,
            opts ...etcdapi.OpOption) goffkv.Watch {

    opts = append(opts, etcdapi.WithRev(header.Revision + 1))

    ctx, ctxCancelFunc := context.WithCancel(context.Background())
    ch := c.client.Watch(ctx, key, opts...)
    return func() {
        <-ch
        ctxCancelFunc()
    }
}

func (c *etcdClient) getLease() (etcdapi.LeaseID, error) {
    if c.leaseId != etcdapi.NoLease {
        return c.leaseId, nil
    }

    resp, err := c.client.Grant(context.TODO(), leaseTtlSeconds)
    if err != nil {
        return etcdapi.NoLease, err
    }

    c.leaseId = resp.ID
    c.leaseKaDoneCh = make(chan interface{})

    go func() {
        timeout := leaseTtlSeconds * time.Second / 2
        for {
            select {
            case <-c.leaseKaDoneCh:
                return
            case <-time.After(timeout):
                _, _ = c.client.KeepAliveOnce(context.TODO(), c.leaseId)
            }
        }
    }()

    return c.leaseId, nil
}

func (c *etcdClient) Create(key string, value []byte, lease bool) (goffkv.Version, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    td := txnDraft{}
    _ = c.addParentExistsCheck(segments, &td)
    td.addCheck(c.checkNotExists(segments))

    leaseId := etcdapi.NoLease
    if lease {
        leaseId, err = c.getLease()
        if err != nil {
            return 0, err
        }
    }

    td.doOnSuccess(c.put(segments, value, etcdapi.WithLease(leaseId)))
    td.doOnFailure(c.get(segments))

    m, err := c.commitTxnDraft(td)
    if err != nil {
        return 0, err
    }

    if m.Succeeded {
        return 1, nil
    }

    if rrEmpty(m.Responses[0]) {
        return 0, goffkv.OpErrNoEntry
    }
    return 0, goffkv.OpErrEntryExists
}

func (c *etcdClient) Set(key string, value []byte) (goffkv.Version, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    expectLease := false
    for {
        td := txnDraft{}
        _ = c.addParentExistsCheck(segments, &td)

        putOpts := []etcdapi.OpOption{}

        if expectLease {
            putOpts = append(putOpts, etcdapi.WithIgnoreLease())
        } else {
            td.addCheck(c.checkLeaseId(segments, etcdapi.NoLease))
        }

        td.doOnSuccess(c.put(segments, value, putOpts...))

        td.doOnSuccess(c.get(segments))

        m, err := c.commitTxnDraft(td)
        if err != nil {
            return 0, err
        }

        if m.Succeeded {
            return rrExtractVersion(m.Responses[1]), nil
        }

        if len(m.Responses) > 0 && rrEmpty(m.Responses[0]) {
            return 0, goffkv.OpErrNoEntry
        }

        expectLease = !expectLease
    }
}

func unwrapKey(rawKey []byte, nGlobalPrefix int) string {
    rawKey = rawKey[nGlobalPrefix:]
    for i, c := range rawKey {
        if c == '\x00' {
            return string(rawKey[:i]) + string(rawKey[i + 1:])
        }
    }
    panic("No NUL byte found")
}

func (c *etcdClient) Children(key string, watch bool) ([]string, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return nil, nil, err
    }

    kr := c.directChildrenRange(segments)

    td := txnDraft{}
    td.addCheck(c.checkExists(segments))
    td.doOnSuccess(c.getRange(kr, etcdapi.WithKeysOnly()))

    m, err := c.commitTxnDraft(td)
    if err != nil {
        return nil, nil, err
    }

    if !m.Succeeded {
        return nil, nil, goffkv.OpErrNoEntry
    }

    rr := m.Responses[0].GetResponseRange()

    result := []string{}
    nGlobalPrefix := len(c.assemblePathNormally(nil))
    for _, kv := range rr.Kvs {
        result = append(result, unwrapKey(kv.Key, nGlobalPrefix))
    }

    var resultWatch goffkv.Watch
    if watch {
        resultWatch = c.makeWatch(rr.Header, kr.left, etcdapi.WithRange(kr.right))
    }
    return result, resultWatch, nil
}

func (c *etcdClient) Exists(key string, watch bool) (goffkv.Version, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, nil, err
    }

    resp, err := c.client.Do(context.TODO(), c.get(segments, etcdapi.WithKeysOnly()))
    if err != nil {
        return 0, nil, err
    }

    gr := resp.Get()

    var resultVer uint64
    var resultWatch goffkv.Watch
    if gr.Count > 0 {
        resultVer = uint64(gr.Kvs[0].Version)
    }
    if watch {
        resultWatch = c.makeWatch(gr.Header, c.assembleKeyWithNul(segments))
    }
    return resultVer, resultWatch, nil
}

func (c *etcdClient) Cas(key string, value []byte, ver goffkv.Version) (goffkv.Version, error) {
    if ver == 0 {
        resultVer, err := c.Create(key, value, false)
        if err == nil {
            return resultVer, nil
        }
        if err == goffkv.OpErrEntryExists {
            return 0, nil
        }
        return 0, err
    }

    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    td := txnDraft{}

    td.addCheck(c.checkVersion(segments, int64(ver)))

    td.doOnSuccess(c.put(segments, value, etcdapi.WithIgnoreLease()))
    td.doOnSuccess(c.get(segments, etcdapi.WithKeysOnly()))

    td.doOnFailure(c.get(segments, etcdapi.WithKeysOnly()))

    m, err := c.commitTxnDraft(td)
    if err != nil {
        return 0, err
    }

    if m.Succeeded {
        return rrExtractVersion(m.Responses[1]), nil
    }

    if rrEmpty(m.Responses[0]) {
        return 0, goffkv.OpErrNoEntry
    }
    return 0, nil
}

func (c *etcdClient) Erase(key string, ver goffkv.Version) error {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return err
    }

    td := txnDraft{}

    if ver != 0 {
        td.addCheck(c.checkVersion(segments, int64(ver)))
    } else {
        td.addCheck(c.checkExists(segments))
    }

    td.doOnSuccess(c.del(segments))
    td.doOnSuccess(c.delRange(c.subtreeRange(segments)))

    td.doOnFailure(c.get(segments, etcdapi.WithKeysOnly()))

    m, err := c.commitTxnDraft(td)
    if err != nil {
        return err
    }

    if m.Succeeded {
        return nil
    }
    if rrEmpty(m.Responses[0]) {
        return goffkv.OpErrNoEntry
    }
    return nil
}

func (c *etcdClient) Get(key string, watch bool) (goffkv.Version, []byte, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, nil, nil, err
    }

    resp, err := c.client.Do(context.TODO(), c.get(segments))
    if err != nil {
        return 0, nil, nil, err
    }

    gr := resp.Get()
    if gr.Count == 0 {
        return 0, nil, nil, goffkv.OpErrNoEntry
    }
    kv := gr.Kvs[0]

    resultVer := uint64(kv.Version)
    resultValue := kv.Value
    var resultWatch goffkv.Watch
    if watch {
        resultWatch = c.makeWatch(gr.Header, c.assembleKeyWithNul(segments))
    }
    return resultVer, resultValue, resultWatch, nil
}

func (c *etcdClient) Commit(txn goffkv.Txn) ([]goffkv.TxnOpResult, error) {
    td := txnDraft{}

    ee := [][]bool{}
    setIndices := []int{}
    createIndices := []int{}
    nOps := 0

    for _, check := range txn.Checks {
        segments, err := goffkv.DisassembleKey(check.Key)
        if err != nil {
            return nil, err
        }

        if check.Ver != 0 {
            td.addCheck(c.checkVersion(segments, int64(check.Ver)))
        } else {
            td.addCheck(c.checkExists(segments))
        }
        td.doOnFailure(c.get(segments))
    }

    for _, op := range txn.Ops {
        segments, err := goffkv.DisassembleKey(op.Key)
        if err != nil {
            return nil, err
        }

        e := []bool{}

        switch op.What {
        case goffkv.Create:
            td.addCheck(c.checkNotExists(segments))

            leaseId := etcdapi.NoLease
            if op.Lease {
                leaseId, err = c.getLease()
                if err != nil {
                    return nil, err
                }
            }
            td.doOnSuccess(c.put(segments, op.Value, etcdapi.WithLease(leaseId)))
            td.doOnFailure(c.get(segments, etcdapi.WithKeysOnly()))
            e = append(e, false)

            if c.addParentExistsCheck(segments, &td) {
                e = append(e, true)
            }

            createIndices = append(createIndices, nOps)

        case goffkv.Set:
            td.addCheck(c.checkExists(segments))
            td.doOnSuccess(c.put(segments, op.Value, etcdapi.WithIgnoreLease()))
            td.doOnSuccess(c.get(segments))
            td.doOnFailure(c.get(segments, etcdapi.WithKeysOnly()))
            e = append(e, true)

            nOps++
            setIndices = append(setIndices, nOps)

        case goffkv.Erase:
            td.addCheck(c.checkExists(segments))
            td.doOnSuccess(c.del(segments))
            td.doOnSuccess(c.delRange(c.subtreeRange(segments)))
            td.doOnFailure(c.get(segments))
            e = append(e, true)
        }

        ee = append(ee, e)
        nOps++
    }

    m, err := c.commitTxnDraft(td)
    if err != nil {
        return nil, err
    }

    if m.Succeeded {
        result := []goffkv.TxnOpResult{}
        for i, j := 0, 0; i != len(createIndices) || j != len(setIndices); {
            if j == len(setIndices) || createIndices[i] < setIndices[j] {
                result = append(result, goffkv.TxnOpResult{
                    goffkv.Create,
                    1,
                })
                i++
            } else {
                result = append(result, goffkv.TxnOpResult{
                    goffkv.Set,
                    rrExtractVersion(m.Responses[setIndices[j]]),
                })
                j++
            }
        }
        return result, nil

    } else {
        for i, check := range txn.Checks {
            r := m.Responses[i]
            if rrEmpty(r) || rrExtractVersion(r) != check.Ver {
                return nil, goffkv.TxnError{i}
            }
        }

        j := 0
        for i, _ := range txn.Ops {
            for _, expected := range ee[i] {
                got := !rrEmpty(m.Responses[j + len(txn.Checks)])
                if expected != got {
                    return nil, goffkv.TxnError{i + len(txn.Checks)}
                }
                j++
            }
        }

        panic("cannot recover index of failed txn op")
    }
}

func (c *etcdClient) Close() {
    if c.leaseKaDoneCh != nil {
        close(c.leaseKaDoneCh)
    }
    c.client.Close()
}

func init() {
    goffkv.RegisterClient("etcd", New)
}

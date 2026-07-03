package etcd_api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/teamones-open/etcd-discovery/common"
	"go.etcd.io/etcd/clientv3"
)

const (
	_ttl = 30
)

type Register struct {
	cli       *clientv3.Client
	leaseId   clientv3.LeaseID
	lease     clientv3.Lease
	info      *NodeInfo
	closeChan chan error

	stopOnce sync.Once
}

func NewRegister(info *NodeInfo, conf clientv3.Config) (reg *Register, err error) {
	r := &Register{}
	r.closeChan = make(chan error)
	r.info = info

	r.cli, err = clientv3.New(conf)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Register) Run() {
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()

	log.Printf("[Register] Run start node:%s addr:%s", r.info.UniqueId, r.info.Addr)

	if err := r.register(); err != nil {
		log.Printf("[Register] initial register failed: %+v", err)
		return
	}

	for {
		select {
		case <-timer.C:
			if err := r.keepAlive(); err != nil {
				log.Printf("[Register] keepAlive failed: %+v", err)
			}

		case <-r.closeChan:
			log.Printf("[Register] Run exit node:%s", r.info.UniqueId)
			return
		}
	}
}

func (r *Register) Stop() {
	r.stopOnce.Do(func() {
		if err := r.revoke(); err != nil {
			log.Printf("[Register] revoke failed: %+v", err)
		}

		close(r.closeChan)

		if r.cli != nil {
			_ = r.cli.Close()
		}
	})
}

func (r *Register) register() error {
	r.leaseId = 0

	kv := clientv3.NewKV(r.cli)
	r.lease = clientv3.NewLease(r.cli)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leaseResp, err := r.lease.Grant(ctx, _ttl)
	if err != nil {
		return errors.Wrap(err, "[Register] register Grant err")
	}

	data, err := json.Marshal(r.info)
	if err != nil {
		return errors.Wrap(err, "[Register] json Marshal err")
	}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()

	_, err = kv.Put(
		putCtx,
		r.info.UniqueId,
		string(data),
		clientv3.WithLease(leaseResp.ID),
	)
	if err != nil {
		return errors.Wrapf(
			err,
			"[Register] register kv.Put err name:%s data:%s",
			r.info.Name,
			string(data),
		)
	}

	r.leaseId = leaseResp.ID

	log.Printf(
		"[Register] registered node:%s leaseId:%d ttl:%d",
		r.info.UniqueId,
		r.leaseId,
		_ttl,
	)

	return nil
}

func (r *Register) keepAlive() error {
	if r.lease == nil || r.leaseId == 0 {
		return errors.Errorf("[Register] invalid lease state leaseId:%d", r.leaseId)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := r.lease.KeepAliveOnce(ctx, r.leaseId)
	if err != nil {
		log.Printf(
			"[Register] keepAlive failed, will re-register. node:%s leaseId:%d err:%+v",
			r.info.UniqueId,
			r.leaseId,
			err,
		)

		if regErr := r.register(); regErr != nil {
			return errors.Wrap(regErr, "[Register] re-register failed")
		}

		return nil
	}

	if common.Mode() == common.DebugMode {
		log.Printf(fmt.Sprintf("[Register] keepalive... leaseId:%+v", r.leaseId))
	}

	return nil
}

func (r *Register) revoke() error {
	if r.cli == nil || r.leaseId == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := r.cli.Revoke(ctx, r.leaseId)
	if err != nil {
		return errors.Wrap(err, "[Register] revoke err")
	}

	if common.Mode() == common.DebugMode {
		log.Printf(fmt.Sprintf("[Register] revoke node:%+v", r.leaseId))
	}

	return nil
}

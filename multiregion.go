package gubernator

import (
	"github.com/mailgun/holster/v3/syncutil"
	"github.com/sirupsen/logrus"
)

type mutliRegionManager struct {
	reqQueue chan *RateLimitReq
	wg       syncutil.WaitGroup
	conf     BehaviorConfig
	log      *logrus.Entry
	instance *Instance
}

func newMultiRegionManager(conf BehaviorConfig, instance *Instance) *mutliRegionManager {
	mm := mutliRegionManager{
		conf:     conf,
		instance: instance,
		reqQueue: make(chan *RateLimitReq, 0),
	}
	mm.runAsyncReqs()
	return &mm
}

// QueueHits writes the RateLimitReq to be asynchronously sent to other regions
func (mm *mutliRegionManager) QueueHits(r *RateLimitReq) {
	mm.reqQueue <- r
}

func (mm *mutliRegionManager) runAsyncReqs() {
	var interval = NewInterval(mm.conf.MultiRegionSyncWait)
	hits := make(map[string]*RateLimitReq)

	mm.wg.Until(func(done chan struct{}) bool {
		select {
		case r := <-mm.reqQueue:
			key := r.HashKey()

			// Aggregate the hits into a single request
			_, ok := hits[key]
			if ok {
				hits[key].Hits += r.Hits
			} else {
				hits[key] = r
			}

			// Send the hits if we reached our batch limit
			if len(hits) == mm.conf.MultiRegionBatchLimit {
				for dc, picker := range mm.instance.GetRegionPickers() {
					log.Infof("Sending %v hit(s) to %s picker", len(hits), dc)
					mm.sendHits(hits, picker)
				}
				hits = make(map[string]*RateLimitReq)
			}

			// Queue next interval
			if len(hits) == 1 {
				interval.Next()
			}

		case <-interval.C:
			if len(hits) > 0 {
				for dc, picker := range mm.instance.GetRegionPickers() {
					log.Infof("Sending %v hit(s) to %s picker", len(hits), dc)
					mm.sendHits(hits, picker)
				}
				hits = make(map[string]*RateLimitReq)
			}

		case <-done:
			return false
		}
		return true
	})
}

// TODO: Sending cross DC should mainly update the hits, the config should not be sent, or ignored when received
// TODO: Calculation of OVERLIMIT should not occur when sending hits cross DC
func (mm *mutliRegionManager) sendHits(r map[string]*RateLimitReq, picker PeerPicker) {
	// Does nothing for now
}

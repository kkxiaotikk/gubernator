/*
Copyright 2018-2019 Mailgun Technologies Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"context"
	"math/rand"
	"time"

	"github.com/mailgun/gubernator"
	"github.com/pkg/errors"
)

type Address struct {
	HTTPAddress string
	GRPCAddress string
}

var daemons []*gubernator.Daemon
var peers []Address

// Returns a random peer from the cluster
func GetRandomPeer() Address {
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
	return peers[0]
}

// Returns a list of all peers in the cluster
func GetPeers() []Address {
	return peers
}

// Returns a list of all deamons in the cluster
func GetDaemons() []*gubernator.Daemon {
	return daemons
}

// Returns a specific peer
func PeerAt(idx int) Address {
	return peers[idx]
}

// Returns a specific daemon
func DaemonAt(idx int) *gubernator.Daemon {
	return daemons[idx]
}

// Start a local cluster of gubernator servers
func Start(numInstances int) error {
	addresses := make([]Address, numInstances, numInstances)
	return StartWith(addresses)
}

// Start a local cluster with specific addresses
func StartWith(addresses []Address) error {
	for _, address := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		d, err := gubernator.NewDaemon(ctx, gubernator.DaemonConfig{
			GRPCListenAddress: address.GRPCAddress,
			HTTPListenAddress: address.HTTPAddress,
			Behaviors: gubernator.BehaviorConfig{
				GlobalSyncWait: time.Millisecond * 50, // Suitable for testing but not production
				GlobalTimeout:  time.Second,
			},
		})
		cancel()
		if err != nil {
			return errors.Wrapf(err, "while starting server for addr '%s'", address)
		}

		// Add the peers and daemons to the package level variables
		peers = append(peers, Address{
			GRPCAddress: d.GRPCListener.Addr().String(),
			HTTPAddress: d.HTTPListener.Addr().String(),
		})
		daemons = append(daemons, d)
	}

	var pi []gubernator.PeerInfo
	for _, p := range peers {
		pi = append(pi, gubernator.PeerInfo{Address: p.GRPCAddress})
	}

	// Tell each instance about the other peers
	for _, d := range daemons {
		d.SetPeers(pi)
	}
	return nil
}

func Stop() {
	for _, d := range daemons {
		d.Close()
	}
	peers = nil
	daemons = nil
}

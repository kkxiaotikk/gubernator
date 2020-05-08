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

package cluster_test

import (
	"testing"

	"github.com/mailgun/gubernator"
	"github.com/mailgun/gubernator/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartMultipleInstances(t *testing.T) {
	err := cluster.Start(2)
	require.Nil(t, err)

	assert.Equal(t, 2, len(cluster.GetPeers()))
	assert.Equal(t, 2, len(cluster.GetDaemons()))
	cluster.Stop()
}

func TestStartZeroInstances(t *testing.T) {
	err := cluster.Start(0)
	require.Nil(t, err)

	assert.Equal(t, 1, len(cluster.GetPeers()))
	assert.Equal(t, 1, len(cluster.GetDaemons()))
	cluster.Stop()
}

func TestStartMultipleInstancesWithAddresses(t *testing.T) {
	addresses := []cluster.Address{{GRPCAddress: "localhost:1111"}, {GRPCAddress: "localhost:2222"}}
	err := cluster.StartWith(addresses)
	require.Nil(t, err)

	wantPeers := []gubernator.PeerInfo{
		{Address: "127.0.0.1:1111"},
		{Address: "127.0.0.1:2222"},
	}

	daemons := cluster.GetDaemons()
	assert.Equal(t, wantPeers, cluster.GetPeers())
	assert.Equal(t, 2, len(daemons))
	assert.Equal(t, "127.0.0.1:1111", daemons[0].GRPCListener.Addr().String())
	assert.Equal(t, "127.0.0.1:2222", daemons[1].GRPCListener.Addr().String())
	assert.Equal(t, "127.0.0.1:2222", cluster.DaemonAt(1).GRPCListener.Addr().String())
	assert.Equal(t, "127.0.0.1:2222", cluster.PeerAt(1).GRPCAddress)
}

func TestStartWithAddressesFail(t *testing.T) {
	err := cluster.StartWith([]cluster.Address{{GRPCAddress: "1111"}})
	assert.NotNil(t, err)
	assert.Nil(t, cluster.GetPeers())
	assert.Nil(t, cluster.GetDaemons())
}

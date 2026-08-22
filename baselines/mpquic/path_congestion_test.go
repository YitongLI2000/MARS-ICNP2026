package quic

import (
	"github.com/lucas-clemente/quic-go/congestion"
	"github.com/lucas-clemente/quic-go/internal/protocol"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Path congestion control", func() {
	newPath := func(pathID protocol.PathID) *path {
		return &path{
			pathID: pathID,
			sess: &session{
				version: protocol.VersionMP,
			},
			rttStats: &congestion.RTTStats{},
		}
	}

	It("uses OLIA on additional paths", func() {
		oliaSenders := make(map[protocol.PathID]*congestion.OliaSender)
		pth := newPath(1)

		controller := pth.newCongestionControl(oliaSenders)

		Expect(controller).To(BeAssignableToTypeOf(&congestion.OliaSender{}))
		Expect(pth.congestionControl).To(Equal(congestionControlOlia))
		Expect(oliaSenders).To(HaveKey(protocol.PathID(1)))
	})

	It("uses CUBIC on the initial path", func() {
		oliaSenders := make(map[protocol.PathID]*congestion.OliaSender)
		pth := newPath(protocol.InitialPathID)

		pth.newCongestionControl(oliaSenders)

		Expect(pth.congestionControl).To(Equal(congestionControlCubic))
		Expect(oliaSenders).To(BeEmpty())
	})
})

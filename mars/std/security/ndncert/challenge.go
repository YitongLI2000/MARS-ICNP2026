package ndncert

import "github.com/named-data/ndnd/std/types/optional"

type ChallengeStatus uint64

const (
	ChallengeStatusBefore    ChallengeStatus = 0
	ChallengeStatusChallenge ChallengeStatus = 1
	ChallengeStatusPending   ChallengeStatus = 2
	ChallengeStatusSuccess   ChallengeStatus = 3
	ChallengeStatusFailure   ChallengeStatus = 4
)

// ParamMap is a map of challenge parameters.
type ParamMap map[string][]byte

// Challenge is the interface for an NDNCERT challenge.
type Challenge interface {
	// Name returns the name of the challenge.
	Name() string

	// Request gets the params of the challenge request.
	// The input provides the params of the previous challenge response.
	// Input is nil for the initial request.
	// Status is for the previous challenge response.
	Request(input ParamMap, status optional.Optional[string]) (ParamMap, error)
}

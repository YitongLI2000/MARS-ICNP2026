package ndncert

import (
	"fmt"

	"github.com/named-data/ndnd/std/types/optional"
)

type ChallengePin struct {
	// Callback to get the code from the user.
	CodeCallback func(status string) string
}

func (*ChallengePin) Name() string {
	return KwPin
}

func (c *ChallengePin) Request(input ParamMap, status optional.Optional[string]) (ParamMap, error) {
	// Validate challenge configuration
	if c.CodeCallback == nil {
		return nil, fmt.Errorf("pin challenge not configured")
	}

	// Initial request parameters
	if input == nil {
		return ParamMap{}, nil
	}

	// Challenge response code
	if s := status.GetOr(""); s == "need-code" || s == "wrong-code" {
		code := c.CodeCallback(s)
		if code == "" {
			return nil, fmt.Errorf("no code provided")
		}

		return ParamMap{
			KwCode: []byte(code),
		}, nil
	}

	// Unknown status
	return nil, fmt.Errorf("unknown input to pin challenge")
}

package models

import (
	"fmt"

	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"
)

// TargetUser for exporting
type TargetUser struct {
	M *protocol.TargetUser
}

//ToCSVLine converts the user into a hive readable CSV line
func (t *TargetUser) ToCSVLine() string {
	return fmt.Sprintf(
		"%v;\n",
		t.M.UserId,
	)
}

// UnmarshalFrom Serializes the TargetUser from bytes
func (t *TargetUser) UnmarshalFrom(b []byte) error {
	t.M = &protocol.TargetUser{}
	return proto.Unmarshal(b, t.M)
}

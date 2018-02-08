package models

import (
	"fmt"

	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"
)

// TargetItem for exporting
type TargetItem struct {
	M *protocol.TargetItem
}

//ToCSVLine converts the user into a hive readable CSV line
func (t *TargetItem) ToCSVLine() string {
	return fmt.Sprintf(
		"%v;\n",
		t.M.ItemId,
	)
}

// UnmarshalFrom Serializes the TargetItem from bytes
func (t *TargetItem) UnmarshalFrom(b []byte) error {
	t.M = &protocol.TargetItem{}
	return proto.Unmarshal(b, t.M)
}

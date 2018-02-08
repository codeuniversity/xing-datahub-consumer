package models

import (
	"fmt"

	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"
)

// Interaction for exporting
type Interaction struct {
	M *protocol.Interaction
}

//ToCSVLine converts the user into a hive readable CSV line
func (i *Interaction) ToCSVLine() string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;\n",
		i.M.UserId,
		i.M.ItemId,
		i.M.InteractionType,
		i.M.CreatedAt,
	)
}

// UnmarshalFrom Serializes the Interaction from bytes
func (i *Interaction) UnmarshalFrom(b []byte) error {
	i.M = &protocol.Interaction{}
	return proto.Unmarshal(b, i.M)
}

package models

import (
	"fmt"

	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"
)

// Item for exporting
type Item struct {
	M *protocol.Item
}

//ToCSVLine converts the user into a hive readable CSV line
func (i *Item) ToCSVLine() string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;\n",
		i.M.Id,
		i.M.Title,
		i.M.CareerLevel,
		i.M.DisciplineId,
		i.M.IndustryId,
		i.M.Country,
		i.M.IsPayed,
		i.M.Region,
		i.M.Latitude,
		i.M.Longitude,
		i.M.Employment,
		arrayHelper(i.M.Tags),
		i.M.CreatedAt,
	)
}

// UnmarshalFrom Serializes the Item from bytes
func (i *Item) UnmarshalFrom(b []byte) error {
	i.M = &protocol.Item{}
	return proto.Unmarshal(b, i.M)
}

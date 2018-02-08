package models

import (
	"fmt"

	"github.com/codeuniversity/xing-datahub-protocol"
	"github.com/golang/protobuf/proto"
)

//User for exporting
type User struct {
	M *protocol.User
}

//ToCSVLine converts the user into a hive readable CSV line
func (u *User) ToCSVLine() string {
	return fmt.Sprintf(
		"%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;%v;\n",
		u.M.Id,
		arrayHelper(u.M.Jobroles),
		u.M.CareerLevel,
		u.M.DisciplineId,
		u.M.IndustryId,
		u.M.Country,
		u.M.Region,
		u.M.ExperienceNEntriesClass,
		u.M.ExperienceYearsExperience,
		u.M.ExperienceYearsInCurrent,
		u.M.EduDegree,
		arrayHelper(u.M.EduFieldofstudies),
		u.M.Wtcj,
		u.M.Premium,
	)
}

// UnmarshalFrom Serializes the User from bytes
func (u *User) UnmarshalFrom(b []byte) error {
	u.M = &protocol.User{}
	return proto.Unmarshal(b, u.M)
}

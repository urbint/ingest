package utils

import (
	. "github.com/smartystreets/goconvey/convey"

	"testing"
)

func TestMapFromStructTag(t *testing.T) {
	type Nested struct {
		Supported bool `mytag:"supported"`
	}

	type Anonymous struct {
		BaseVal   string `mytag:"base"`
		Overwrite bool   `mytag:"overwrite-me,omitempty"`
	}

	type MyStruct struct {
		Anonymous
		Overwrite bool   `mytag:"overwritten-me"`
		Num       int    `mytag:"yay"`
		Empty     int    `mytag:"somethingelse,omitempty"`
		Omit      string `mytag:"-"`
		Default   string
		Nested    Nested
	}

	Convey("MapFromStructTag", t, func() {
		Convey("Builds a map from the specified tag", func() {
			vals := MapFromStructTag(&MyStruct{
				Anonymous: Anonymous{
					BaseVal: "Test",
				},
				Overwrite: true,
				Num:       1,
				Omit:      "Hello",
				Default:   "Default",
				Nested: Nested{
					Supported: true,
				},
			}, "mytag")

			Convey("allows specifying custom field names", func() {
				So(vals["yay"], ShouldEqual, 1)
			})
			Convey("allows ommiting field names", func() {
				_, hasField := vals["Omit"]
				_, hasFieldAlt := vals["-"]

				So(hasField, ShouldBeFalse)
				So(hasFieldAlt, ShouldBeFalse)
			})

			Convey("uses the fields name by default", func() {
				So(vals["Default"], ShouldEqual, "Default")
			})

			Convey("supports nested structs", func() {
				So(vals["Nested"], ShouldNotBeNil)

				nestedMap := vals["Nested"].(map[string]interface{})

				So(nestedMap["supported"], ShouldBeTrue)
			})

			Convey("puts anonymous structs on the top level", func() {
				So(vals["Anonymous"], ShouldBeNil)
				So(vals["base"], ShouldEqual, "Test")

				So(vals["overwritten-me"], ShouldBeTrue)
				_, hadOverwrite := vals["overwrite-me"]
				So(hadOverwrite, ShouldBeFalse)
			})
		})
	})
}

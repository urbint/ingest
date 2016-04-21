package utils

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCopyMap(t *testing.T) {
	src := map[string]interface{}{
		"num": 5,
		"str": "string",
		"nested": map[string]interface{}{
			"val":  1,
			"bool": true,
		},
	}
	Convey("Maps", t, func() {
		Convey("CopyMap", func() {
			Convey("coppies a basic map", func() {
				result, err := CopyMap(src)
				So(err, ShouldBeNil)
				So(&result, ShouldNotPointTo, &src)
				So(result, ShouldResemble, src)
			})
		})

		Convey("TransformMap", func() {
			Convey("with a valid transformation", func() {
				err := TransformMap(src, MapTransform{
					"num":         "transformedNum",
					"str":         "-",
					"nested.val":  "hoist",
					"nested.bool": "bury.inside",
				})

				So(err, ShouldBeNil)

				Convey("handles basic renaming", func() {
					So(src, ShouldNotContainKey, "num")
					So(src["transformedNum"], ShouldEqual, 5)
				})

				Convey("handles omission", func() {
					So(src, ShouldNotContainKey, "str")
					So(src, ShouldNotContainKey, "-")
				})

				Convey("handles hoisting", func() {
					So(src, ShouldContainKey, "hoist")
					So(src["hoist"], ShouldEqual, 1)
					So(src["nested"], ShouldNotContainKey, "val")
				})

				Convey("handles burying", func() {
					So(src, ShouldContainKey, "bury")
					So(src["bury"], ShouldResemble, map[string]interface{}{
						"inside": true,
					})
				})
			})
		})

	})
}

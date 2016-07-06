package convert

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestConvert(t *testing.T) {
	Convey("Conversions", t, func() {
		Convey("ToString", func() {
			Convey("work with strings", func() {
				So(ToString("Hello"), ShouldEqual, "Hello")
			})
			Convey("works with other types", func() {
				So(ToString(1), ShouldEqual, "1")
			})
			Convey("works with pointers", func() {
				So(ToString(nil), ShouldEqual, "")

				var typedPtr *int
				So(ToString(typedPtr), ShouldEqual, "")
			})
		})

		Convey("ToInt", func() {
			Convey("Works with ints", func() {
				So(ToInt(5), ShouldEqual, 5)
			})
			Convey("Works with strings", func() {
				So(ToInt("5"), ShouldEqual, 5)
				So(ToInt("5.00"), ShouldEqual, 5)
				So(ToInt("1,005.00"), ShouldEqual, 1005)
			})
		})

		Convey("ToBool", func() {
			Convey("Works with bools", func() {
				So(ToBool(true), ShouldEqual, true)
			})

			Convey("Works with int", func() {
				So(ToBool(5), ShouldEqual, true)
				So(ToBool(0), ShouldEqual, false)
			})

			Convey("Works with string", func() {
				So(ToBool("true"), ShouldEqual, true)
				So(ToBool("false"), ShouldEqual, false)
				So(ToBool(""), ShouldEqual, false)
				So(ToBool("abcd"), ShouldEqual, true)
			})
		})

		Convey("ToFloat32", func() {
			Convey("Works with float32", func() {
				So(ToFloat32(float32(1.4)), ShouldEqual, float32(1.4))
			})
			Convey("Works with float64", func() {
				So(ToFloat32(float64(1.4)), ShouldEqual, float32(1.4))
			})
			Convey("Works with int", func() {
				So(ToFloat32(1), ShouldEqual, float32(1))
			})

			Convey("Works with string", func() {
				So(ToFloat32("1,543.42"), ShouldEqual, float32(1543.42))
			})
		})

		Convey("ToFloat64", func() {
			Convey("Works with float32", func() {
				So(ToFloat64(float64(1.4)), ShouldEqual, float64(1.4))
			})
			Convey("Works with float64", func() {
				So(ToFloat64(float64(1.4)), ShouldEqual, float64(1.4))
			})
			Convey("Works with int", func() {
				So(ToFloat64(1), ShouldEqual, float64(1))
			})

			Convey("Works with string", func() {
				So(ToFloat64("1,543.42"), ShouldEqual, float64(1543.42))
			})
		})

		Convey("ToTime", func() {
			Convey("Works with strings", func() {
				Convey("Handles a default format", func() {
					So(ToTime("2016-05-17T22:34:28.323Z"), ShouldHappenAfter, time.Time{})
					So(ToTime("2016-05-17T22:34:28.323Z"), ShouldHappenBefore, time.Now())
				})
				Convey("Handles a custom format", func() {
					So(ToTime("2006-15-10", "2006-02-01"), ShouldHappenAfter, time.Time{})
					So(ToTime("2006-15-10", "2006-02-01"), ShouldHappenBefore, time.Now())
				})
			})
		})

		Convey("MapToStruct", func() {
			data := map[string]interface{}{
				"Works": true,
				"Name": map[string]interface{}{
					"First": "Bobby",
					"Last":  "Johnson",
				},
				"Age":              42,
				"FriendCount":      "58",
				"FavoriteFloat":    "32.8",
				"Birthday":         "2016-05-17T22:34:28.323Z",
				"CustomDate":       "2010-05-01",
				"PointerDate":      "2000-01-10",
				"PointerDateNoFmt": "2016-05-17",
			}

			type Name struct {
				First string
				Last  string
			}

			type Anonymous struct {
				Works bool
			}

			type Person struct {
				Anonymous
				Name             Name
				AltName          *Name
				Age              int `source:"age"`
				FriendCount      int
				FavoriteFloat    float32
				Birthday         time.Time
				CustomDate       time.Time  `format:"2006-01-02"`
				PointerDate      *time.Time `format:"2006-01-02"`
				PointerDateNoFmt *time.Time
			}

			p := &Person{}

			MapToStruct(data, p)

			Convey("Works with Scalars", func() {
				So(p.Age, ShouldEqual, 42)
			})
			Convey("Applies type conversions", func() {
				So(p.FriendCount, ShouldEqual, 58)
				So(p.FavoriteFloat, ShouldEqual, float32(32.8))
			})
			Convey("Supports embeded structs", func() {
				So(p.Name.First, ShouldEqual, "Bobby")
				So(p.Name.Last, ShouldEqual, "Johnson")
			})

			Convey("Supports anonymous structs", func() {
				So(p.Works, ShouldEqual, true)
			})

			Convey("Supports pointers", func() {
				result := MapToStruct(map[string]interface{}{
					"AltName": map[string]interface{}{
						"First": "Ron",
					},
				}, &Person{}).(*Person)

				So(p.AltName, ShouldBeNil)
				So(result.AltName, ShouldNotBeNil)
				So(result.AltName.First, ShouldEqual, "Ron")
			})

			Convey("Supports specifying struct tags", func() {
				result := MapToStruct(map[string]interface{}{
					"age": 37,
				}, &Person{}, "source").(*Person)

				So(result.Age, ShouldEqual, 37)
			})

			Convey("Supports time parsing", func() {
				Convey("uses a default time format", func() {
					So(p.Birthday, ShouldHappenAfter, time.Time{})
					So(p.Birthday, ShouldHappenBefore, time.Now())
				})
				Convey("handles times as a pointer", func() {
					So(*p.PointerDate, ShouldHappenAfter, time.Time{})
					So(*p.PointerDate, ShouldHappenBefore, time.Now())
					So(*p.PointerDateNoFmt, ShouldHappenAfter, time.Time{})
					So(*p.PointerDateNoFmt, ShouldHappenBefore, time.Now())
				})
				Convey("allows configuration of a custom time format", nil)
			})
		})
	})
}

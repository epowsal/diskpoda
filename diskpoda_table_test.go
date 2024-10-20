package diskpoda

import (
	"testing"
)

func TestTable0(t *testing.T) {
	opt := DefaultDiskpodaOption(1<<40, 512)
	dp, pde := OpenDiskpoda("C:/work/testout/dipoda1", opt)
	if pde != nil {
		panic(pde)
	}
	if dp.diskf == 0 {
		panic("open disk error")
	}
	if pde != nil {
		panic(pde)
	}
	brainboot := dp.OpenTable("brainoot", 0, 0)
	brainboot.Execute(`
xiao1 is int equal 34
xiao2 is uint32
xiao3 equal xiao1 add xiao2
if xiao1 is 34, xiao2 is 0:
	xiao3 equal 6
i is int
for i less than 10:
	i equal i add 1
	print i
function sum:
	values int,uint,float
	code:
		out is int64
		for v in values:
			out equal out add v
		return out
db set "uuhhd" xiao1
oop equal db get "dddd"
//数据结构
sego is lawyer,teacher:
	age current year-birth day
	birth day 1988
for i is 0 to 9:
	print i
`, nil)

	brainboot.Close()

	dp.Close()

}

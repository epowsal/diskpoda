package diskpoda

import (
	"bytes"
	"e"
	"io/ioutil"
	"math/rand"
	"testing"

	"linbo.ga/toolfunc"
)

// func Test0(t *testing.T) {
// 	opt := DefaultDiskpodaOption(1<<40, 512)
// 	dp, pde := OpenDiskpoda("C:/work/testout/dipoda1", opt)
// if pde != nil {
// 	panic(pde)
// }
// 	if dp.diskf == 0 {
// 		panic("open disk error")
// 	}
// 	if pde != nil {
// 		panic(pde)
// 	}

// 	addr := dp.GetSpace(4096)
// 	dp.PutSpace(4096, addr)
// 	addr1 := dp.GetSpace(4096)
// 	if addr1 != addr || addr1 == 0 || addr == 0 {
// 		e.P("addr1 != addr", addr1, addr)
// 		panic("error")
// 	}
// 	e.P(addr)
// 	dp.Close()
// }

func Test1(t *testing.T) {
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
	id := dp.AllocId()
	valtxt := "qwerty" + toolfunc.Uint32ToStr(id)
	addr, saer := dp.PutData("abc", 0, 0, []byte(valtxt), 0)
	if saer != nil {
		panic(saer)
	}
	dp.AddHash("abc", addr)
	data := dp.GetData("abc", 0, 0, nil)
	if data == nil {
		panic("read error")
	}
	if bytes.Compare(data, []byte(valtxt)) != 0 {
		e.P(data, valtxt)
		e.P([]byte("qwerty"), string(data))
		panic("error")
	}
	dp.AddId(id, addr)
	data = dp.GetData("abc", id, 0, nil)
	if data == nil {
		panic("read error")
	}
	if bytes.Compare(data, []byte(valtxt)) != 0 {
		e.P(data, valtxt)
		e.P([]byte("qwerty"), string(valtxt))
		panic("error")
	}
	addr1, saer1 := dp.PutData("abc", 0, 0, []byte("qwertyqwertyqwertyqwertyqwertyqwertyqwertyqwerty"), 0)
	if saer1 != nil {
		panic(saer1)
	}
	if addr1 != addr {
		dp.AddHash("abc", addr1)
		dp.AddId(id, addr1)
	}
	data1 := dp.GetData("", 0, addr1, nil)
	if data1 == nil {
		panic("read error")
	}
	if bytes.Compare(data1, []byte("qwertyqwertyqwertyqwertyqwertyqwertyqwertyqwerty")) != 0 {
		panic("error")
	}

	st := dp.OpenStream("file0", 0, 0, 4096, 0)
	st.Write(8, []byte("abcdefghijklmn"))
	outbs := make([]byte, 4)
	st.Read(10, outbs)
	if bytes.Compare(outbs, []byte("cdef")) != 0 {
		e.P(outbs, []byte("cdef"))
		panic("error")
	}
	staddr := st.Addr()
	if staddr == 0 {
		panic("error")
	}
	st.Close()

	st1 := dp.OpenStream("file1", 0, 0, 4096, 0)
	w8k := make([]byte, 4096*2)
	w8kb := byte('a' + rand.Intn(26))
	for i := 0; i < 4096*2; i += 1 {
		w8k[i] = w8kb
	}
	st1.Write(17, []byte(w8k[:5200]))
	out8kbs := make([]byte, 4096*2)
	st1.Read(17, out8kbs[:5200])
	if bytes.Compare(out8kbs[:5200], []byte(w8k[:5200])) != 0 {
		ioutil.WriteFile("testout/w8k", w8k[:5200], 0666)
		ioutil.WriteFile("testout/out8kbs", out8kbs[:5200], 0666)
		panic("error")
	}
	staddr1 := st1.Addr()
	if staddr1 == 0 {
		panic("error")
	}
	st1.Close()

	st2 := dp.OpenStream("file2", 0, 0, 4096, 0)
	w12k := make([]byte, 4096*3)
	w12kb := byte('a' + rand.Intn(26))
	for i := 0; i < 4096*3; i += 1 {
		w12k[i] = w12kb
	}
	st2.Write(17, []byte(w12k[:8888]))
	out12kbs := make([]byte, 4096*3)
	st2.Read(17, out12kbs[:8888])
	if bytes.Compare(out12kbs[:8888], []byte(w12k[:8888])) != 0 {
		ioutil.WriteFile("testout/w12k", w12k[:8888], 0666)
		ioutil.WriteFile("testout/out12kbs", out12kbs[:8888], 0666)
		panic("error")
	}
	staddr2 := st2.Addr()
	if staddr2 == 0 {
		panic("error")
	}
	st2.Close()

	st3 := dp.OpenStream("file3", 0, 0, 4096, 0)
	w13k := make([]byte, 4096*3)
	w13kb := byte('a' + rand.Intn(26))
	for i := 0; i < 4096*3; i += 1 {
		w13k[i] = w13kb
	}
	st3.Write(17, []byte(w13k[:4096*3]))
	w14kb := byte('a' + rand.Intn(26))
	for i := 3000; i < 10000; i += 1 {
		w13k[i] = w14kb
	}
	st3.Write(3017, []byte(w13k[3000:10000]))
	out13kbs := make([]byte, 4096*3)
	st3.Read(17, out13kbs[:4096*3])
	if bytes.Compare(out13kbs[:4096*3], []byte(w13k[:4096*3])) != 0 {
		ioutil.WriteFile("testout/w13k", w13k[:4096*3], 0666)
		ioutil.WriteFile("testout/out13kbs", out13kbs[:4096*3], 0666)
		panic("error")
	}
	staddr3 := st3.Addr()
	if staddr3 == 0 {
		panic("error")
	}
	st3.Close()

	st4 := dp.OpenStream("file4", 0, 0, 4096, 0)
	w14k := make([]byte, 4096*3)
	w14kb1 := byte('a' + rand.Intn(26))
	for i := 0; i < 4096*2; i += 1 {
		w14k[i] = w14kb1
	}
	st4.Write(17, []byte(w14k[:4096*2]))
	w14kb2 := byte('a' + rand.Intn(26))
	for i := 3000; i < 10000; i += 1 {
		w14k[i] = w14kb2
	}
	st4.Write(3017, []byte(w14k[3000:10000]))
	out14kbs := make([]byte, 4096*3)
	st4.Read(17, out14kbs[:10000])
	if bytes.Compare(out14kbs[:10000], []byte(w14k[:10000])) != 0 {
		ioutil.WriteFile("testout/w14k", w14k[:10000], 0666)
		ioutil.WriteFile("testout/out14kbs", out14kbs[:10000], 0666)
		panic("error")
	}

	st4.Truncate(int64(st4.BlockMaxDataSize()))
	out14kbs = st4.ReadAll(out14kbs)
	r4rl := []byte(toolfunc.JoinBytes(make([]byte, 17), w14k[0:st4.BlockMaxDataSize()-17]))
	if bytes.Compare(out14kbs, r4rl) != 0 {
		ioutil.WriteFile("testout/w14k", r4rl, 0666)
		ioutil.WriteFile("testout/out14kbs", out14kbs, 0666)
		panic("error")
	}

	staddr4 := st4.Addr()
	if staddr4 == 0 {
		panic("error")
	}
	st4.Close()

	st5 := dp.OpenStream("file5", 0, 0, 4096, 0)
	w15k := make([]byte, 4096*3)
	w15kb1 := byte('a' + rand.Intn(26))
	for i := 0; i < 3000; i += 1 {
		w15k[i] = w15kb1
	}
	st5.Write(17, []byte(w15k[:3000]))
	w15kb2 := byte('a' + rand.Intn(26))
	for i := 10000; i < 4096*3; i += 1 {
		w15k[i] = w15kb2
	}
	st5.Write(10017, []byte(w15k[10000:4096*3]))
	out15kbs := make([]byte, 4096*3)
	st5.Read(17, out15kbs[:4096*3])
	if bytes.Compare(out15kbs[:3000], []byte(w15k[:3000])) != 0 {
		ioutil.WriteFile("testout/w15k", w15k[:4096*3], 0666)
		ioutil.WriteFile("testout/out15kbs", out15kbs[:4096*3], 0666)
		panic("error")
	}
	if bytes.Compare(out15kbs[10000:4096*3], []byte(w15k[10000:4096*3])) != 0 {
		ioutil.WriteFile("testout/w15k", w15k[:4096*3], 0666)
		ioutil.WriteFile("testout/out15kbs", out15kbs[:4096*3], 0666)
		panic("error")
	}

	st5.Truncate(-int64(st5.Size()) + int64(st5.BlockMaxDataSize()))
	out15kbs = st5.Read(0, out15kbs[:4096*3])
	if bytes.Compare(out15kbs[len(out15kbs)-(4096*3-10000):], []byte(w15k[len(w15k)-(4096*3-10000):])) != 0 {
		ioutil.WriteFile("testout/w15k", w15k[st5.BlockMaxDataSize()-17:4096*3], 0666)
		ioutil.WriteFile("testout/out15kbs", out15kbs, 0666)
		panic("error")
	}
	ioutil.WriteFile("testout/out15fukbs", out15kbs, 0666)

	staddr5 := st5.Addr()
	if staddr5 == 0 {
		panic("error")
	}

	st5.Close()

	dp.Close()
	//Clear("testout/dipoda1")

	//reopen
	dp, pde = OpenDiskpoda("C:/work/testout/dipoda1", opt)
	if pde != nil {
		panic(pde)
	}
	if dp.diskf == 0 {
		panic("open disk error")
	}
	if pde != nil {
		panic(pde)
	}

	abcdata1 := dp.GetData("", 0, addr1, nil)
	if abcdata1 == nil {
		panic("read error")
	}
	if bytes.Compare(abcdata1, []byte("qwertyqwertyqwertyqwertyqwertyqwertyqwertyqwerty")) != 0 {
		panic("error")
	}

	abcdata2 := dp.GetData("abc", 0, 0, nil)
	if abcdata2 == nil {
		panic("read error")
	}
	if bytes.Compare(abcdata2, []byte("qwertyqwertyqwertyqwertyqwertyqwertyqwertyqwerty")) != 0 {
		panic("error")
	}

	st4 = dp.OpenStream("file4", 0, 0, 4096, 0)
	e.P("st4 size", st4.Size())
	out14kbs = st4.ReadAll(out14kbs)
	if bytes.Compare(out14kbs, r4rl) != 0 {
		ioutil.WriteFile("testout/w14k", r4rl, 0666)
		ioutil.WriteFile("testout/out14kbs", out14kbs, 0666)
		panic("error")
	}
	st4.Close()

	st5 = dp.OpenStream("file5", 0, 0, 4096, 0)
	out15kbs = st5.Read(0, out15kbs[:4096*3])
	if bytes.Compare(out15kbs[len(out15kbs)-(4096*3-10000):], []byte(w15k[len(w15k)-(4096*3-10000):])) != 0 {
		ioutil.WriteFile("testout/w15k", w15k[st5.BlockMaxDataSize()-17:4096*3], 0666)
		ioutil.WriteFile("testout/out15kbs", out15kbs, 0666)
		panic("error")
	}
	st5.Close()

	dp.Close()

}

package diskpoda

import (
	"bufio"
	"bytes"
	"compress/flate"
	"e"
	"io"
)

func (dp *Diskpoda) flateEncode(buf, src []byte, level int, blockaxdasize uint64) []byte {
	b := bytes.NewBuffer(buf)
	b.Reset()
	zw, err := flate.NewWriter(b, 1)
	if err != nil {
		e.Pa(4, "flateEncode flate.NewWriter error.")
	}
	zw.Write(src)
	zw.Close()
	if b.Len() >= int(blockaxdasize) { //算法决定不能删除
		buf = buf[:len(src)]
		copy(buf, src)
		return buf
	}
	return b.Bytes()
}

func (dp *Diskpoda) flateEncodeV2(b *bytes.Buffer, flatw *flate.Writer, buf, src []byte, level int, blockaxdasize uint64) []byte {
	flatw.Reset(b)
	flatw.Write(src)
	flatw.Close()
	if b.Len() >= int(blockaxdasize) { //算法决定不能删除
		buf = buf[:len(src)]
		copy(buf, src)
		return buf
	}
	return b.Bytes()
}

func (dp *Diskpoda) flateDecode(outbuf, src []byte, blockaxdasize uint64) []byte {
	if len(src) == 0 {
		return []byte{}
	}
	if len(src) == int(blockaxdasize) { //算法决定不能删除
		outbuf = outbuf[:len(src)]
		copy(outbuf, src)
		return outbuf
	}
	b := bytes.NewBuffer(src)
	zr := flate.NewReader(nil)
	if err := zr.(flate.Resetter).Reset(b, nil); err != nil {
		e.Pa(4, "flateDecode flate Reset error")
	}
	b2 := bytes.NewBuffer(outbuf)
	b2.Reset()
	outw := bufio.NewWriter(b2)
	if _, err := io.Copy(outw, zr); err != nil {
		e.Pa(4, "feflateDecode io.Copy error", src)
	}
	if err := zr.Close(); err != nil {
		e.Pa(4, "flateDecode close error", src)
	}
	return b2.Bytes()
}

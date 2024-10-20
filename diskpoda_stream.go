package diskpoda

import (
	"bytespool"
	"e"

	"github.com/emirpasic/gods/maps/treemap"
	"linbo.ga/toolfunc"
)

type Stream struct {
	dp         *Diskpoda
	name       string
	id         uint32
	tbks       *treemap.Map[uint64, *StreamBlockInfo]
	blocksize  uint64
	iscompress uint64
	bonebkfile uint64
}

func (dp *Diskpoda) OpenStream(name string, id uint32, addr, blocksize uint64, bcompress byte) (ds *Stream) {
	/*
		每次打开后面写入最小块4kb
		是否是流,上一个是否是尾巴块地址,未定义,未定义,3byte剩余表示数据长度;容量;上下块;
	*/
	ds = &Stream{dp: dp, name: name, id: id}
	ds.iscompress = uint64(bcompress)
	if blocksize < 4096 {
		blocksize = 4096
	}
	if blocksize%4096 != 0 {
		blocksize = (blocksize / 4096) * 4096
	}
	if ds.iscompress == 0 {
		ds.blocksize = blocksize
	} else {
		ds.blocksize = blocksize * 4
	}

	ds.tbks = treemap.New[uint64, *StreamBlockInfo]()
	if addr > 0 {
		ds.loadAllBlock(addr)
		return ds
	}
	if id > 0 {
		idbitst := dp.Opt.SectorSize*8 + 512*1024*1024*8 + uint64(id)*dp.Opt.SizeBitCnt
		idbited := idbitst + dp.Opt.SizeBitCnt
		var idbytest, idbyteed uint64
		if idbitst%8 == 0 {
			idbytest = idbitst / 8
		} else {
			idbytest = idbitst / 8
		}
		if idbited%8 == 0 {
			idbyteed = idbited / 8
		} else {
			idbyteed = idbited/8 + 1
		}
		rdbuf := bytespool.Get(int(idbyteed - idbytest))
		dp.diskRead(idbytest, rdbuf)
		idaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt)
		if idaddr == 0 {
			return ds
		} else {
			ds.loadAllBlock(idaddr)
			return ds
		}
	}
	if name != "" {
		h32 := uint64(toolfunc.BKDRHash32([]byte(name)))
		idbitst := dp.Opt.SectorSize*8 + 512*1024*1024*8 + dp.DiskStepSize((1<<32)*dp.Opt.SizeBitCnt/8)*8 + h32*(dp.Opt.SizeBitCnt+1)
		idbited := idbitst + dp.Opt.SizeBitCnt + 1
		var idbytest, idbyteed uint64
		if idbitst%8 == 0 {
			idbytest = idbitst / 8
		} else {
			idbytest = idbitst / 8
		}
		if idbited%8 == 0 {
			idbyteed = idbited / 8
		} else {
			idbyteed = idbited/8 + 1
		}
		rdbuf := bytespool.Get(int(idbyteed - idbytest))
		defer bytespool.Put(rdbuf)
		dp.diskRead(idbytest, rdbuf)
		blist := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+1)
		haaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1)
		if blist == 1 {
			kbs := dp.blockRead(haaddr, nil)
			nameaddr := dp.nameAddrFind(kbs, name)
			if nameaddr == 0 {
				return ds
			} else {
				ds.loadAllBlock(nameaddr)
				return ds
			}
		} else {
			if haaddr == 0 {
				return ds
			} else {
				ds.loadAllBlock(haaddr)
				return ds
			}
		}
	}
	return nil
}

func (ds *Stream) loadAllBlock(firstaddr uint64) (err error) {
	tmp := bytespool.Get(int(ds.dp.Opt.SectorSize))
	sfpos := uint64(0)
	curaddr := firstaddr
	for true {
		ds.dp.diskRead(curaddr, tmp)
		head := ds.dp.streamBlockHead(curaddr, tmp)
		if curaddr == firstaddr {
			if head.bstream == 1 {
				if (head.pre<<2)>>42 != 0 {
					ds.blocksize = (head.pre << 2) >> 42 //22it流数据块大小
				}
				ds.tbks.Put(sfpos, head)
				curaddr = head.next
				sfpos += (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
				ds.bonebkfile = 0
				continue
			} else {
				if (head.pre<<2)>>42 != 0 {
					ds.blocksize = (head.pre << 2) >> 42 //22bit单块数据大小
				}
				ds.tbks.Put(sfpos, head)
				ds.bonebkfile = 1
				return nil
			}
		}
		ds.tbks.Put(sfpos, head)
		if head.bstream != 0 {
			curaddr = head.next
			sfpos += (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		} else {
			return nil
		}
	}
	return nil
}

func (ds *Stream) Size() (n uint64) {
	_, mav, bmaok := ds.tbks.Max()
	if bmaok {
		return mav.next
	}
	return 0
}

func (ds *Stream) appendZero(sflastaddr, dabsfst, dabsfed uint64) (lastv *StreamBlockInfo) {
	if sflastaddr == 0 {
		e.Pa(4, "appedZero error")
	}
	_, mav, bmaok := ds.tbks.Max()
	if bmaok == false {
		return nil
	}
	tmp := bytespool.Gen(int(ds.blocksize), int(ds.blocksize))

	preaddr := uint64(0)
	curaddr := uint64(0)
	nextaddr := uint64(0)
	sfcurpos := dabsfst
	if sfcurpos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) != 0 {
		sfcurpos = sfcurpos + (ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - sfcurpos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)
	}
	if dabsfed < sfcurpos {
		ds.dp.diskRead(sflastaddr, tmp)
		for i2 := dabsfed % ds.blocksize; i2 < sfcurpos%ds.blocksize; i2 += 1 {
			tmp[i2] = 0
		}
		toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen*2, dabsfed)
		ds.dp.diskWrite(sflastaddr, tmp)
		return mav
	}
	bfirst := true
	firstaddr := uint64(0)
	curaddr = ds.dp.GetSpace(ds.blocksize)
nextwrite:
	if sfcurpos+(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) <= dabsfed {
		nextaddr = ds.dp.GetSpace(ds.blocksize)
	} else {
		nextaddr = dabsfed
	}
	toolfunc.BytesSetBits(tmp, 0, 2, uint64(0b10))
	toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, preaddr)
	toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.TwoAddrbytelen*8, nextaddr)
	ds.dp.diskWrite(curaddr, tmp)
	if bfirst {
		bfirst = false
		firstaddr = curaddr
	}
	tbkspresize := ds.tbks.Size()
	if sfcurpos == 0 {
		toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen, ds.dp.Opt.BlockSizeByteCnt*8, ds.blocksize)
	}
	curaddrv := &StreamBlockInfo{addr: curaddr, pre: preaddr, next: nextaddr, bstream: 1, bcompress: 0}
	ds.tbks.Put(sfcurpos, curaddrv)
	if tbkspresize == 0 {
		if ds.name != "" {
			ds.dp.AddHash(ds.name, curaddr)
		}
		if ds.id != 0 {
			ds.dp.AddId(ds.id, curaddr)
		}
	}
	if sfcurpos+(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) >= dabsfed {
		//写头部连接块
		ds.dp.diskRead(sflastaddr, tmp)
		bst := dabsfst % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		for i2 := uint64(0); i2 < ds.blocksize-(ds.dp.Opt.TwoAddrbytelen+bst); i2 += 1 {
			tmp[ds.dp.Opt.TwoAddrbytelen+bst+i2] = 0
		}
		toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.TwoAddrbytelen*8, firstaddr)
		mav.next = firstaddr
		ds.dp.diskWrite(sflastaddr, tmp)
		bytespool.Put(tmp)
		return curaddrv
	}
	sfcurpos += (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
	preaddr = curaddr
	curaddr = nextaddr
	goto nextwrite
	bytespool.Put(tmp)
	return nil
}

func (ds *Stream) Write(pos uint64, dabs []byte) (er error) {
	tmp := bytespool.Gen(int(ds.blocksize), int(ds.blocksize))
	_, mav, bmaok := ds.tbks.Max()
	if bmaok && pos == ^uint64(0) {
		pos = mav.next
	} else if pos == ^uint64(0) {
		pos = 0
	}
	/*
		情况:
		0.空流文件
		1.在一个扇区
			1.1.扇区存在
			1.2.扇区不存在
		2.大于1个扇区
			2.1.全有
			2.2.部分有
			2.3.全部没有
	*/
	var dastartaddrv *StreamBlockInfo
	daendaddr := uint64(0)
	if bmaok && pos+lbs(dabs) <= mav.next {
		// midpos := (pos + pos + lbs(dabs)) / 2
		// if midpos < mav.next/2 {
		// 	daendaddr, _ = ds.readBlockBaseInfoNextTo(pos + lbs(dabs))
		// } else {
		// 	dastartaddr, _ = ds.readBlockBaseInfoPrevTo(pos)
		// }
		_, dastartaddrv, _ = ds.tbks.Floor(pos)
		_, clv, _ := ds.tbks.Floor(pos + lbs(dabs))
		daendaddr = clv.addr
	} else if bmaok && pos <= mav.next {
		// dastartaddr, _ = ds.readBlockBaseInfoNextTo(pos)
		// ds.readBlockBaseInfoPrevTo(pos)
		_, dastartaddrv, _ = ds.tbks.Floor(pos)
	} else if bmaok {
		e.P("mav.next", mav.next, pos, mav)
		dastartaddrv = ds.appendZero(mav.addr, mav.next, pos)
	}

	preaddr := uint64(0)
	if bmaok {
		if pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) != 0 {
			_, leftv, _ := ds.tbks.Floor(pos)
			preaddr = leftv.addr
		} else if pos-1 > 0 {
			_, leftv, _ := ds.tbks.Floor(pos - 1)
			preaddr = leftv.addr
		}
	}
	sfcurbkpos := uint64(0)
	sfsgbkst := uint64(0)
	curaddr := uint64(0)
	dabscur := uint64(0)
	bwsinglebk := true
	if pos-pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) <= pos+lbs(dabs) && pos+lbs(dabs) <= pos+((ds.blocksize-ds.dp.Opt.TwoAddrbytelen)-pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)) {
		//单block
		if dastartaddrv != nil && dastartaddrv.addr != 0 {
			curaddr = dastartaddrv.addr
		} else {
			curaddr = ds.dp.GetSpace(ds.blocksize)
		}
		sfcurbkpos = pos - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)
		sfsgbkst = pos % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		dabscur = 0
	} else {
		if pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) != 0 {
			sfcurbkpos = pos + (ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)
			dabscur = (ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - (pos % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen))
		} else {
			sfcurbkpos = pos
			dabscur = 0
		}
		curaddr = ds.dp.GetSpace(ds.blocksize)
		bwsinglebk = false
	}
	if pos-pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) == 0 && bmaok == false {
		bwsinglebk = true
		dabscur = 0
		sfcurbkpos = 0
		curaddr = ds.dp.GetSpace(ds.blocksize)
		sfsgbkst = pos % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
	}
	nextaddr := uint64(0)
	bfirstaddr := true
	firstaddr := uint64(0)

writenext:
	dabsusebscnt := uint64(0)
	if dabscur+(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) >= lbs(dabs) {
		if daendaddr == 0 {
			nextaddr = pos + lbs(dabs)
			copy(tmp[ds.dp.Opt.TwoAddrbytelen+sfsgbkst:], dabs[dabscur:len(dabs)])
			toolfunc.BytesSetBits(tmp, 0, 2, 0b00)
		} else {
			ds.dp.diskRead(daendaddr, tmp)
			e.P("sfsgbkst", sfsgbkst, dabscur, dabs, tmp)
			e.P(tmp[ds.dp.Opt.TwoAddrbytelen+sfsgbkst:])
			e.P(dabs[dabscur:len(dabs)])
			copy(tmp[ds.dp.Opt.TwoAddrbytelen+sfsgbkst:], dabs[dabscur:len(dabs)])
			nextaddr = toolfunc.BytesGetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen*2)
			if bwsinglebk {
				curaddr = daendaddr
			}
			toolfunc.BytesSetBits(tmp, 0, 2, 0b10)
		}
	} else {
		nextaddr = ds.dp.GetSpace(ds.blocksize)
		copy(tmp[ds.dp.Opt.TwoAddrbytelen+sfsgbkst:], dabs[dabscur:dabscur+(ds.blocksize-ds.dp.Opt.TwoAddrbytelen-sfsgbkst)])
		dabsusebscnt = (ds.blocksize - ds.dp.Opt.TwoAddrbytelen - sfsgbkst)
		toolfunc.BytesSetBits(tmp, 0, 2, 0b10)
	}

	toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, preaddr)
	toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.TwoAddrbytelen*8, nextaddr)
	if curaddr%ds.dp.Opt.SectorSize != 0 {
		panic("sector align error")
	}
	if bfirstaddr {
		bfirstaddr = false
		firstaddr = curaddr
	}
	e.P("writeaddr", curaddr, "sfcurbkpos", sfcurbkpos, "len dabs", lbs(dabs), "ds.tbks size", ds.tbks.Keys(), tmp)
	odbki, bobki := ds.tbks.Get(sfcurbkpos)
	if bobki && odbki.addr != curaddr {
		if odbki.bcompress == 0 {
			ds.dp.PutSpace(ds.blocksize, odbki.addr)
		} else {
			ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+odbki.datasize, odbki.addr)
		}
	}
	tbkspresize := ds.tbks.Size()
	if sfcurbkpos == 0 {
		toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen, ds.dp.Opt.BlockSizeByteCnt*8, ds.blocksize)
	}
	dwn := ds.dp.diskWrite(curaddr, tmp)
	if dwn != nil {
		return nil
	}
	ds.tbks.Put(sfcurbkpos, &StreamBlockInfo{addr: curaddr, pre: preaddr, next: nextaddr, bstream: 1, bcompress: ds.iscompress})
	if tbkspresize == 0 {
		if ds.name != "" {
			ds.dp.AddHash(ds.name, curaddr)
		}
		if ds.id != 0 {
			ds.dp.AddId(ds.id, curaddr)
		}
	}
	if dabscur+(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) >= lbs(dabs) {
		if bwsinglebk == false && dastartaddrv != nil && dastartaddrv.addr != 0 {
			if dastartaddrv.addr == 0 {
				e.Pa(4, "dastartaddrv.addr == 0 error")
			} else {
				ds.dp.diskRead(dastartaddrv.addr, tmp)
			}
			copy(tmp[ds.dp.Opt.TwoAddrbytelen+pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen):], dabs[:(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)-pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)])
			toolfunc.BytesSetBits(tmp, 0, ds.dp.Opt.AddrPrefixbitlen, 0b10)
			if bmaok == false {
				e.Pa(4, "bmaok==false error")
			}
			toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.TwoAddrbytelen*8, firstaddr)
			dastartaddrv.next = firstaddr
			ds.dp.diskWrite(dastartaddrv.addr, tmp)
			return nil
		}
		e.P("writeaddr", curaddr, "len dabs", lbs(dabs), "ds.tbks size", ds.tbks.Keys())
		return nil
	}
	dabscur += dabsusebscnt
	preaddr = curaddr
	curaddr = nextaddr
	nextaddr = 0
	sfsgbkst = 0
	sfcurbkpos += (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
	goto writenext
	return e.E(4, "return position error")
}

func (ds *Stream) WriteAll(dabs []byte) (er error) {
	ds.Truncate(0)
	return ds.Write(0, dabs)
}

// if cap(buf)<output bytespool.Put(output);
func (ds *Stream) ReadAll(outbsbuf []byte) (outbs []byte) {
	_, mav, bma := ds.tbks.Max()
	if bma == true && cbs(outbsbuf) < mav.next || bma == false {
		outbsbuf := bytespool.Get(int(mav.next))
		return ds.Read(0, outbsbuf)
	} else {
		e.P("mav.next", mav.next)
		return ds.Read(0, outbsbuf[:mav.next])
	}
}

func (ds *Stream) Read(pos uint64, outbsbuf []byte) (obs []byte) {
	_, mav, bma := ds.tbks.Max()
	if bma == false || len(outbsbuf) == 0 {
		return nil
	}
	tmp := bytespool.Get(int(ds.blocksize))
	var bkbsstart uint64 = ds.dp.Opt.TwoAddrbytelen + pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)
	var sfcurbkpos uint64 = pos - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)
	var outbsbufcur, outbsbufcurend uint64
	var rdbkstart, rdbkend uint64
	rdbkstart = pos % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
	if lbs(outbsbuf) > ((ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)) {
		rdbkend = (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		outbsbufcurend = (ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)
	} else if lbs(outbsbuf) == ((ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen)) {
		rdbkend = (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		outbsbufcurend = lbs(outbsbuf)
	} else {
		rdbkend = (pos + lbs(outbsbuf)) % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		outbsbufcurend = lbs(outbsbuf)
	}

	for true {
		_, bki, bkie := ds.tbks.Floor(sfcurbkpos)
		if bkie == false {
			e.P(bki, bkie, pos, sfcurbkpos, ds.tbks.Keys())
			return nil
		}
		ds.dp.ReadSector(bki.addr, tmp)
		e.P("readaddr", bki.addr, "outbsbufcur:outbsbufcurend", outbsbufcur, outbsbufcurend, rdbkstart, rdbkend, "bkbsstart", bkbsstart, "bki.next", bki.next, "outbsbufcur", outbsbufcur, outbsbufcurend, bkbsstart, ds.tbks.Size(), "pos", pos, ds.tbks.Keys(), tmp)
		copy(outbsbuf[outbsbufcur:outbsbufcurend], tmp[ds.dp.Opt.TwoAddrbytelen+rdbkstart:ds.dp.Opt.TwoAddrbytelen+rdbkend])
		if lbs(outbsbuf) == outbsbufcurend {
			bytespool.Put(tmp)
			return outbsbuf
		} else if sfcurbkpos+rdbkend >= mav.next {
			bytespool.Put(tmp)
			outbsbuf = outbsbuf[:outbsbufcur+rdbkend]
			return outbsbuf
		}
		sfcurbkpos += (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		outbsbufcur = outbsbufcurend
		if sfcurbkpos+(ds.blocksize-ds.dp.Opt.TwoAddrbytelen) < mav.next {
			outbsbufcurend += (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
			rdbkend = (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		} else {
			outbsbufcurend += mav.next % ds.BlockMaxDataSize()
			rdbkend = mav.next % ds.BlockMaxDataSize()
		}
		rdbkstart = 0

		if outbsbufcurend >= lbs(outbsbuf) {
			outbsbufcurend = lbs(outbsbuf)
			rdbkend = (lbs(outbsbuf) - ((ds.blocksize - ds.dp.Opt.TwoAddrbytelen) - pos%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen))) % (ds.blocksize - ds.dp.Opt.TwoAddrbytelen)
		}
	}
	return nil
}

// >=0正向保留; <0 count from end;必须与ds.blocksize对齐;
func (ds *Stream) Truncate(newsize int64) (er error) {
	if newsize >= 0 {
		_, mav, bma := ds.tbks.Max()
		if bma {
			if uint64(newsize) >= mav.next {
				return nil
			}
			if uint64(newsize)%ds.BlockMaxDataSize() != 0 {
				if uint64(newsize)+uint64(newsize)-uint64(newsize)%ds.BlockMaxDataSize() >= mav.next {
					return nil
				} else {
					newsize += newsize - newsize%int64(ds.BlockMaxDataSize())
				}
			}
			for nowsize := uint64(newsize); nowsize < uint64(mav.next); nowsize += ds.BlockMaxDataSize() {
				k := nowsize
				v, bv := ds.tbks.Get(k)
				if bv {
					if nowsize == 0 {
						if ds.name != "" {
							ds.dp.DelHash(ds.name, v.addr)
						}
						if ds.id != 0 {
							ds.dp.DelId(ds.id, v.addr)
						}
					}
					if v.bstream == 1 {
						if v.bcompress == 1 {
							ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+v.datasize, v.addr)
						} else {
							ds.dp.PutSpace(ds.blocksize, v.addr)
						}
					} else {
						if v.bcompress == 1 {
							ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+v.datasize, v.addr)
						} else {
							ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+v.next%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen), v.addr)
						}
					}
					ds.tbks.Remove(k)
				}
			}
			_, mav, bmaok := ds.tbks.Max()
			if bmaok == false {
				return nil
			}
			mav.next = uint64(newsize)
			tmp := bytespool.Get(int(ds.dp.Opt.SectorSize))
			ds.dp.ReadSector(mav.addr, tmp)
			toolfunc.BytesSetBits(tmp, 0, ds.dp.Opt.AddrPrefixbitlen, uint64(0b00))
			toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.TwoAddrbytelen*8, uint64(newsize))
			ds.dp.WriteSector(mav.addr, tmp)
			bytespool.Put(tmp)
			return nil
		}
	} else {
		_, mav, bma := ds.tbks.Max()
		if bma {
			headsize := int64(mav.next) + newsize
			headsize = headsize - headsize%int64(ds.BlockMaxDataSize())
			if headsize > 0 {
				for nowsize := uint64(0); nowsize < uint64(headsize); nowsize += ds.BlockMaxDataSize() {
					k := nowsize
					v, bv := ds.tbks.Get(k)
					if bv {
						if v.bstream == 1 {
							if v.bcompress == 1 {
								ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+v.datasize, v.addr)
							} else {
								ds.dp.PutSpace(ds.blocksize, v.addr)
							}
						} else {
							if v.bcompress == 1 {
								ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+v.datasize, v.addr)
							} else {
								ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+v.next%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen), v.addr)
							}
						}
						e.P("remove k", k, "headsize", headsize)
						ds.tbks.Remove(k)
					}
				}

				for nowsize := uint64(headsize); nowsize <= mav.next-mav.next%ds.BlockMaxDataSize(); nowsize += ds.BlockMaxDataSize() {
					k := nowsize
					v, bv := ds.tbks.Get(k)
					e.P("resetpos", k, v, bv, ds.tbks.Keys())
					if bv {
						if k == uint64(headsize) {
							if v.addr != 0 {
								tmp := bytespool.Get(int(ds.dp.Opt.SectorSize))
								ds.dp.ReadSector(v.addr, tmp)
								toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen, ds.dp.Opt.BlockSizeByteCnt*8, ds.blocksize)
								ds.dp.WriteSector(v.addr, tmp)
								bytespool.Put(tmp)
								if ds.name != "" {
									ds.dp.AddHash(ds.name, v.addr)
								}
								if ds.id != 0 {
									ds.dp.AddId(ds.id, v.addr)
								}
							} else {
								e.Pa(4, "v.addr==0")
							}
						}
						ds.tbks.Remove(k)
						ds.tbks.Put(k-uint64(headsize), v)
						if nowsize == mav.next-mav.next%ds.BlockMaxDataSize() {
							v.next = uint64(-newsize)
						}
					}
				}
				tmp := bytespool.Get(int(ds.dp.Opt.SectorSize))
				ds.dp.ReadSector(mav.addr, tmp)
				if ds.tbks.Size() == 1 {
					toolfunc.BytesSetBits(tmp, 0, ds.dp.Opt.AddrPrefixbitlen, uint64(0b00))
					toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen, ds.dp.Opt.BlockSizeByteCnt*8, uint64(ds.blocksize))
				}
				toolfunc.BytesSetBits(tmp, ds.dp.Opt.AddrPrefixbitlen+ds.dp.Opt.Addrbitlen, ds.dp.Opt.TwoAddrbytelen*8, uint64(-newsize))
				ds.dp.WriteSector(mav.addr, tmp)
				bytespool.Put(tmp)
				return nil
			}
		}
	}
	return nil
}

func (ds *Stream) Addr() (addr uint64) {
	_, miv, bmiok := ds.tbks.Min()
	if bmiok == true {
		e.P(miv.addr)
		return miv.addr
	} else {
		return 0
	}
}

func (ds *Stream) releaseAllSpae() (err error) {
	ds.tbks.All(func(k uint64, v *StreamBlockInfo) bool {
		if v.bstream == 1 {
			if v.bcompress == 1 {
				ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+v.datasize, v.addr)
			} else {
				ds.dp.PutSpace(ds.blocksize, v.addr)
			}
		} else {
			if v.bcompress == 1 {
				ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+ds.dp.Opt.BlockSizeByteCnt+v.datasize, v.addr)
			} else {
				ds.dp.PutSpace(ds.dp.Opt.TwoAddrbytelen+v.next%(ds.blocksize-ds.dp.Opt.TwoAddrbytelen), v.addr)
			}
		}
		return true
	})
	return nil
}

func (ds *Stream) BlockMaxDataSize() (siz uint64) {
	return ds.blocksize - ds.dp.Opt.TwoAddrbytelen
}

func (ds *Stream) Close() (er error) {
	ds.tbks.Clear()
	return er
}

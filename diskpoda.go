// diskpoda project diskpoda.go iwlb@outlook.com 2024/09/19
// disk partition as a database.
package diskpoda

// disk poda is for partition or file alloc id or save load data
// 1秒钟就刷入,1秒钟内能发生的事情很少;

import (
	"bytes"
	"bytespool"
	"e"
	"encoding/binary"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"linbo.ga/toolfunc"
)

func init() {

}

type Diskpoda struct {
	Path      string
	diskf     uint64
	Opt       *DiskpodaOption
	IdBlocks  [][]byte
	Num_block map[uint32]*SpaceBlockInfo

	SyncChan           chan int
	stepsiz_nforsector map[uint32]uint16
	bdev               bool
}

type DiskpodaOption struct {
	FullSize     uint64
	SectorSize   uint64 //should be 512;ssd最小读写单元;
	IdSegCur     uint64 //address have 512MB;max 2^32 ID value;
	SizeBitCnt   uint64
	FreeSpaceCur uint64

	//free space num nums
	NumnumsBlockSize               uint64
	MaxModelVal                    uint64 //must equal or great than physical sector byte size;
	MaxBlockSize                   uint64 //stream file or single block file max block size;
	TwoAddrbytelen                 uint64 //default 11byte;
	AddrPrefixbitlen               uint64 //AddrPrefixbitlen+Addrbitlen*2=TwoAddrbytelen*8;
	Addrbitlen                     uint64 //AddrPrefixbitlen+Addrbitlen*2=TwoAddrbytelen*8;
	BlockSizeByteCnt               uint64 //3byte;
	UselessStreamBlockSize         uint64 //存储修改极少或者每次全部不一样的数据大于4兆的数据256kb一块的流文件.
	DefaultStreamBlockSize         uint64 //4096byte;
	DefaultStreamCompressBlockSize uint64 //4*4096byte;
}

type DataBlockInfo struct {
	addr      uint64
	bstream   uint64
	bcompress uint64
	size      uint64
}

type StreamBlockInfo struct {
	addr      uint64
	pre       uint64
	next      uint64
	bstream   uint64
	bcompress uint64
	datasize  uint64 //compress true the data size;block head: 2bit+addrit*2+ksizebytecnt;
}

func DefaultDiskpodaOption(partitionsize, sectorsize uint64) *DiskpodaOption {
	opt := &DiskpodaOption{}
	opt.SectorSize = sectorsize  //512;
	opt.FullSize = partitionsize //1<<40 1TB;
	opt.IdSegCur = 0
	opt.SizeBitCnt = toolfunc.GetSizeBitCnt(opt.FullSize)
	//free space num nums
	opt.NumnumsBlockSize = 4096
	opt.MaxModelVal = opt.SectorSize
	opt.MaxBlockSize = 4 * 1024 * 1024
	opt.TwoAddrbytelen = 11                        //default 11byte suport 8TB disk;
	opt.AddrPrefixbitlen = 2                       //0bit:stream|single block;1bit:1 compressed and have data size segment 3 byte;AddrPrefixbitlen+Addrbitlen=Addrbitlen*2*8;
	opt.Addrbitlen = 43                            //AddrPrefixbitlen+Addrbitlen=Addrbitlen*2*8;
	opt.BlockSizeByteCnt = 3                       //2-24bit;0-2s head;
	opt.UselessStreamBlockSize = 256 * 1024        //存储修改极少或者每次全部不一样的数据大于4兆的数据256kb一块的流文件.
	opt.DefaultStreamBlockSize = 4 * 1024          //4096byte;
	opt.DefaultStreamCompressBlockSize = 16 * 1024 //4*4096byte;
	return opt
}

// 设计就是name id常用来处理搜索执行用.
// 需要融入:dealloc_numnums,name_id,named_named_id,
// 结构:文件系统分区信息扇区;ID找地址表:1/32*2^32ID*6byte地址信息第一块;4byte哈希*2^32*6byte名字地址或者名字地址列表的地址24GB;被释放空间记录数据开始;第一个可以分配名字数据开始地址;
// id 4byte表示,时间4byte表示(现实必须带时间);
// 流会实际写入硬盘,缓存两边,numnus全部缓存,idaddrs全部缓存,hashaddr全部缓存,idseg全部缓存,缓存的都是小数据,Sync前写入个压缩的硬盘备份;
func OpenDiskpoda(path string, opt *DiskpodaOption) (*Diskpoda, error) {
	dp := &Diskpoda{Path: toolfunc.ToAbsolutePath(path), Opt: opt, SyncChan: make(chan int, 0)}
	dp.IdBlocks = make([][]byte, 1024)
	e.P(runtime.GOOS)
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(dp.Path, "\\\\.") { //like:\\\\.\\H:
			dp.bdev = true
		}
	} else {
		if strings.HasPrefix(dp.Path, "/dev/") {
			dp.bdev = true
		}
	}
	dp.Num_block = make(map[uint32]*SpaceBlockInfo, 0)
	dp.stepsiz_nforsector = make(map[uint32]uint16, 0)
	for i := uint64(1); i <= dp.Opt.MaxBlockSize; i += 1 {
		ss := dp.DiskStepSize(uint64(i))
		dp.stepsiz_nforsector[uint32(ss)] = 0
		for j := uint16(1); j <= 512; j += 1 {
			if (ss*uint64(j))%dp.Opt.SectorSize == 0 {
				dp.stepsiz_nforsector[uint32(ss)] = uint16(j)
				break
			}
		}
	}
	e.P(len(dp.stepsiz_nforsector))
	bdiskexists := true
	if dp.bdev == false {
		st, ste := os.Stat(dp.Path)
		if ste != nil {
			os.MkdirAll(toolfunc.GetFilePathDir(dp.Path), 0666)
			bdiskexists = false
		} else {
			minsiz := uint64(opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*toolfunc.GetSizeBitCnt(opt.FullSize)/8, opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(toolfunc.GetSizeBitCnt(opt.FullSize)+1)/8, opt.SectorSize) + toolfunc.GetStepSize(uint64(len(dp.stepsiz_nforsector))*(6+2), dp.Opt.SectorSize)
			if uint64(st.Size()) < minsiz {
				return nil, e.E(8, "file exist but it is not diskpodafile")
			}
		}
	}
	dp.diskf = OpenPartition(dp.Path)

	if bdiskexists == false {
		dp.Opt.FreeSpaceCur = uint64(opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*toolfunc.GetSizeBitCnt(opt.FullSize)/8, opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(toolfunc.GetSizeBitCnt(opt.FullSize)+1)/8, opt.SectorSize) + toolfunc.GetStepSize(uint64(len(dp.stepsiz_nforsector))*(6+2), dp.Opt.SectorSize)

		dp.initFirstSectorOption()
		dp.initIdStateBlock()
		dp.initIdToAddrBlock()
		dp.initHashToAdAndAdLsBlock()
		dp.initNumnums()

		if dp.bdev == false {
			dp.diskfSync()
		}
	} else {
		dp.readFirstSectorOption()
		dp.readNumnumsHead()
	}
	e.P(dp.diskf)

	return dp, nil
}

func (dp *Diskpoda) initFirstSectorOption() {
	e.P(dp.Opt.SectorSize)
	sec := bytespool.Gen(int(dp.Opt.SectorSize), int(dp.Opt.SectorSize))
	defer bytespool.Put(sec)
	fsh := []byte("wlbdiskpoda\x00")
	fshl := lbs(fsh)
	copy(sec, []byte(fsh))

	toolfunc.BytesSetBits(sec, fshl*8+0*64, fshl*8+1*64, dp.Opt.FullSize)
	toolfunc.BytesSetBits(sec, fshl*8+1*64, fshl*8+2*64, dp.Opt.SectorSize)
	toolfunc.BytesSetBits(sec, fshl*8+2*64, fshl*8+3*64, dp.Opt.IdSegCur)
	toolfunc.BytesSetBits(sec, fshl*8+3*64, fshl*8+4*64, dp.Opt.SizeBitCnt)
	toolfunc.BytesSetBits(sec, fshl*8+4*64, fshl*8+5*64, dp.Opt.FreeSpaceCur)
	toolfunc.BytesSetBits(sec, fshl*8+5*64, fshl*8+6*64, dp.Opt.NumnumsBlockSize)
	toolfunc.BytesSetBits(sec, fshl*8+6*64, fshl*8+7*64, dp.Opt.MaxModelVal)
	toolfunc.BytesSetBits(sec, fshl*8+7*64, fshl*8+8*64, dp.Opt.MaxBlockSize)
	toolfunc.BytesSetBits(sec, fshl*8+8*64, fshl*8+9*64, dp.Opt.TwoAddrbytelen)
	toolfunc.BytesSetBits(sec, fshl*8+9*64, fshl*8+10*64, dp.Opt.AddrPrefixbitlen)
	toolfunc.BytesSetBits(sec, fshl*8+10*64, fshl*8+11*64, dp.Opt.Addrbitlen)
	toolfunc.BytesSetBits(sec, fshl*8+11*64, fshl*8+12*64, dp.Opt.BlockSizeByteCnt)
	toolfunc.BytesSetBits(sec, fshl*8+12*64, fshl*8+13*64, dp.Opt.UselessStreamBlockSize)
	toolfunc.BytesSetBits(sec, fshl*8+13*64, fshl*8+14*64, dp.Opt.DefaultStreamBlockSize)

	dp.WriteSector(0, sec)
}

func (dp *Diskpoda) readFirstSectorOption() {
	sec := bytespool.Get(int(dp.Opt.SectorSize))
	dp.ReadSector(0, sec)
	fsh := []byte("wlbdiskpoda\x00")
	fshl := lbs(fsh)
	dp.Opt.FullSize = toolfunc.BytesGetBits(sec, fshl*8+0*64, fshl*8+1*64)
	dp.Opt.SectorSize = toolfunc.BytesGetBits(sec, fshl*8+1*64, fshl*8+2*64)
	dp.Opt.IdSegCur = toolfunc.BytesGetBits(sec, fshl*8+2*64, fshl*8+3*64)
	e.P("dp.Opt.IdSegCur", dp.Opt.IdSegCur)
	dp.Opt.SizeBitCnt = toolfunc.BytesGetBits(sec, fshl*8+3*64, fshl*8+4*64)
	dp.Opt.FreeSpaceCur = toolfunc.BytesGetBits(sec, fshl*8+4*64, fshl*8+5*64)
	dp.Opt.NumnumsBlockSize = toolfunc.BytesGetBits(sec, fshl*8+5*64, fshl*8+6*64)
	dp.Opt.MaxModelVal = toolfunc.BytesGetBits(sec, fshl*8+6*64, fshl*8+7*64)
	dp.Opt.MaxBlockSize = toolfunc.BytesGetBits(sec, fshl*8+7*64, fshl*8+8*64)
	dp.Opt.TwoAddrbytelen = toolfunc.BytesGetBits(sec, fshl*8+8*64, fshl*8+9*64)
	dp.Opt.AddrPrefixbitlen = toolfunc.BytesGetBits(sec, fshl*8+9*64, fshl*8+10*64)
	dp.Opt.Addrbitlen = toolfunc.BytesGetBits(sec, fshl*8+10*64, fshl*8+11*64)
	dp.Opt.BlockSizeByteCnt = toolfunc.BytesGetBits(sec, fshl*8+11*64, fshl*8+12*64)
	dp.Opt.UselessStreamBlockSize = toolfunc.BytesGetBits(sec, fshl*8+12*64, fshl*8+13*64)
	dp.Opt.DefaultStreamBlockSize = toolfunc.BytesGetBits(sec, fshl*8+13*64, fshl*8+14*64)
	e.P("dp.Opt", dp.Opt)
}

func (dp *Diskpoda) initIdStateBlock() {
	secsiz := int(dp.Opt.SectorSize)
	sec := bytespool.Gen(secsiz, secsiz)
	endpos := dp.Opt.SectorSize + 512*1024*1024
	// for i:=dp.Opt.SectorSize;i<endpos;i+=secsiz {
	// 	dp.WriteSector(i, sec)
	// }
	wpos := endpos - uint64(secsiz)
	dp.WriteSector(wpos, sec)
	bytespool.Put(sec)
}

func (dp *Diskpoda) initIdToAddrBlock() {
	secsiz := int(dp.Opt.SectorSize)
	sec := bytespool.Gen(secsiz, secsiz)
	endpos := uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize)
	// for i:=uint64(dp.Opt.SectorSize) + 512*1024*1024;i<endpos;i+=secsiz {
	// 	dp.WriteSector(i, sec)
	// }
	wpos := endpos - uint64(secsiz)
	dp.WriteSector(wpos, sec)
	bytespool.Put(sec)
}

func (dp *Diskpoda) initHashToAdAndAdLsBlock() {
	secsiz := int(dp.Opt.SectorSize)
	sec := bytespool.Gen(secsiz, secsiz)
	endpos := uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(dp.Opt.SizeBitCnt+1)/8, dp.Opt.SectorSize)
	// for i:=uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize);i<endpos;i+=secsiz {
	// 	dp.WriteSector(i, sec)
	// }
	wpos := endpos - uint64(secsiz)
	dp.WriteSector(wpos, sec)
	bytespool.Put(sec)
}

func (dp *Diskpoda) initNumnums() {
	/*
		   for orderfile dp.Opt.Numlen+dp.Opt.Numsilen=64  default:19+45  bit
		```   gor orderfilepeta dp.Opt.Numlen+dp.Opt.Numsilen=80 default:25+55  bit

		   for orderfile Nums sequence: 48 bit 6byte
		   gor orderfilepeta Nums sequence: 56  bit  7byte
		   databitl:可以取的值:40(256g db),48(<256tb fb),56(1pb db),64(16 eksabajt; orderilepeta max can be 4zetabajt)
	*/
	secsiz := int(dp.Opt.SectorSize)
	sec := bytespool.Gen(secsiz, secsiz)
	endpos := uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(dp.Opt.SizeBitCnt+1)/8, dp.Opt.SectorSize) + toolfunc.GetStepSize(uint64(len(dp.stepsiz_nforsector))*(6+2), dp.Opt.SectorSize)
	for i := uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(dp.Opt.SizeBitCnt+1)/8, dp.Opt.SectorSize); i < endpos; i += uint64(secsiz) {
		dp.WriteSector(i, sec)
	}
	wpos := endpos - uint64(secsiz)
	dp.WriteSector(wpos, sec)
	bytespool.Put(sec)
}

func (dp *Diskpoda) readNumnumsHead() {
	e.P("dp.Opt.SizeBitCnt", dp.Opt.SizeBitCnt, dp.Opt.SectorSize)
	stpos := uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(dp.Opt.SizeBitCnt+1)/8, dp.Opt.SectorSize)
	nnsize := toolfunc.GetStepSize(uint64(len(dp.stepsiz_nforsector))*(6+2), dp.Opt.SectorSize)
	sec := bytespool.Gen(int(nnsize), int(nnsize))
	dp.diskRead(stpos, sec)
	e.P("stpos", stpos)
	keys := []int{}
	for k, _ := range dp.stepsiz_nforsector {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for i := uint64(0); i < uint64(len(keys)); i += 1 {
		//64bit=addrbitnt+
		key := keys[i]
		addrcur := toolfunc.BytesGetBits(sec, i*64, (i+1)*64)
		if addrcur>>16 != 0 {
			bki := &SpaceBlockInfo{}
			bki.blockpos = addrcur >> 16
			bki.addrcur = addrcur & 0xffff
			bki.block = bytespool.Get(int(dp.Opt.NumnumsBlockSize))
			dp.diskRead(bki.blockpos, bki.block)
			dp.Num_block[uint32(key)] = bki
		}
	}
}

func (dp *Diskpoda) writeNumnumsHead() {
	stpos := uint64(dp.Opt.SectorSize) + 512*1024*1024 + toolfunc.GetStepSize((1<<32)*dp.Opt.SizeBitCnt/8, dp.Opt.SectorSize) + toolfunc.GetStepSize((1<<32)*(dp.Opt.SizeBitCnt+1)/8, dp.Opt.SectorSize)
	nnsize := toolfunc.GetStepSize(uint64(len(dp.stepsiz_nforsector))*(6+2), dp.Opt.SectorSize)
	sec := bytespool.Gen(int(nnsize), int(nnsize))
	keys := []int{}
	for k, _ := range dp.stepsiz_nforsector {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for i := uint64(0); i < uint64(len(keys)); i += 1 {
		key := keys[i]
		bki, bbk := dp.Num_block[uint32(key)]
		if bbk {
			toolfunc.BytesSetBits(sec, i*64, (i+1)*64, bki.blockpos<<16|bki.addrcur)
		}
	}
	dp.WriteSector(stpos, sec)
	bytespool.Put(sec)
}

func (dp *Diskpoda) AddHash(name string, addr uint64) (er error) {
	haid := uint64(toolfunc.BKDRHash32([]byte(name)))
	idbitst := dp.Opt.SectorSize*8 + 512*1024*1024*8 + dp.DiskStepSize((1<<32)*dp.Opt.SizeBitCnt/8)*8 + uint64(haid)*(dp.Opt.SizeBitCnt+1)
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
	bytespool.Put(rdbuf)
	dp.diskRead(idbytest, rdbuf)
	e.P(rdbuf)
	blist := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+1)
	haaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1)
	if blist == 1 {
		if haaddr == 0 {
			return e.E(4, "list address is 0 error")
		} else {
			/*
				块前2个bit数值情况:
				是否是流,是否压缩
			*/
			kbs := dp.blockRead(haaddr, nil)
			defer bytespool.Put(kbs)
			nlist, _ := dp.nameAddrListAdd(kbs, name, addr)
			defer bytespool.Put(nlist)
			if bytes.Compare(nlist, kbs) == 0 {
				return nil
			} else {
				listaddr := dp.blockWrite(haaddr, []byte(nlist))
				if listaddr != haaddr {
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, listaddr)
					dp.diskWrite(idbytest, rdbuf)
				}
				return nil
			}
		}
	} else {
		if haaddr == 0 {
			toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, addr)
			dp.diskWrite(idbytest, rdbuf)
			return nil
		} else {
			if addr == haaddr {
				return nil
			} else {
				//从haaddr获取名字
				tb := dp.OpenTable("", 0, haaddr)
				//组成两个名字的列表
				hana := tb.Name()
				tb.Close()
				if hana != "" {
					list := "\n" + hana + "\t" + toolfunc.U64ToStr(haaddr) + "\n" + "\n" + name + "\t" + toolfunc.U64ToStr(addr)
					waddr := dp.blockWrite(0, []byte(list))
					toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+1, 1)
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, waddr)
					dp.diskWrite(idbytest, rdbuf)
					return nil
				} else {
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, addr)
					dp.diskWrite(idbytest, rdbuf)
					return nil
				}
			}
		}
	}
	return nil
}

func (dp *Diskpoda) DelHash(name string, addr uint64) (er error) {
	haid := uint64(toolfunc.BKDRHash32([]byte(name)))
	idbitst := dp.Opt.SectorSize*8 + 512*1024*1024*8 + dp.DiskStepSize((1<<32)*dp.Opt.SizeBitCnt/8)*8 + uint64(haid)*(dp.Opt.SizeBitCnt+1)
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
	bytespool.Put(rdbuf)
	dp.diskRead(idbytest, rdbuf)
	e.P(rdbuf)
	blist := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+1)
	haaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1)
	if blist == 1 {
		if haaddr == 0 {
			return e.E(4, "list address is 0 error")
		} else {
			/*
				块前2个bit数值情况:
				是否是流,是否压缩
			*/
			kbs := dp.blockRead(haaddr, nil)
			defer bytespool.Put(kbs)
			nlist, oldaddrs := dp.nameAddrListRemove(kbs, name)
			defer bytespool.Put(nlist)
			if bytes.Compare(nlist, kbs) == 0 {
				return nil
			} else {
				listaddr := dp.blockWrite(haaddr, []byte(nlist))
				if listaddr != haaddr {
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, listaddr)
					dp.diskWrite(idbytest, rdbuf)
				}
				if toolfunc.Uint64FromStr(string(oldaddrs)) != addr {
					e.Pa(4, "DelHash data error", addr, toolfunc.Uint64FromStr(string(oldaddrs)))
				}
				return nil
			}
		}
	} else {
		if haaddr == 0 {
			return nil
		} else {
			if addr == haaddr {
				toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, 0)
				dp.diskWrite(idbytest, rdbuf)
				return nil
			} else {
				return nil
			}
		}
	}
	return nil
}

func (dp *Diskpoda) AddId(id uint32, addr uint64) (er error) {
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
	bytespool.Put(rdbuf)
	dp.diskRead(idbytest, rdbuf)
	toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt, addr)
	dp.diskWrite(idbytest, rdbuf)
	return nil
}

func (dp *Diskpoda) DelId(id uint32, addr uint64) (er error) {
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
	bytespool.Put(rdbuf)
	dp.diskRead(idbytest, rdbuf)
	toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt, 0)
	dp.diskWrite(idbytest, rdbuf)
	return nil
}

/*
防止掉电出错:
要做的事情日志记录下来
记录开始操作
再操作
记录停止操作
*/
func (dp *Diskpoda) PutData(name string, id uint32, addr uint64, data []byte, bcompress byte) (retaddr uint64, er error) {
	if addr > 0 {
		waddr := dp.blockWrite(addr, data)
		return waddr, nil
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
		defer bytespool.Put(rdbuf)
		dp.diskRead(idbytest, rdbuf)
		idaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt)
		if idaddr == 0 {
			waddr := dp.blockWrite(0, data)
			if waddr != 0 {
				toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt, waddr)
				dp.diskWrite(idbytest, rdbuf)
				return waddr, nil
			} else {
				return 0, e.E(8, "block write data error")
			}
		} else {
			waddr := dp.blockWrite(idaddr, data)
			if waddr == idaddr {
				return waddr, nil
			} else {
				toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt, waddr)
				dp.diskWrite(idbytest, rdbuf)
				return waddr, nil
			}
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
			if haaddr == 0 {
				return 0, e.E(4, "list address is 0 error")
			} else {
				/*
					块前2个bit数值情况:
					是否是流,是否压缩
				*/
				kbs := dp.blockRead(haaddr, nil)
				oldaddr := dp.nameAddrFind(kbs, name)
				waddr := dp.blockWrite(oldaddr, data)
				if oldaddr != waddr {
					nlist, _ := dp.nameAddrListAdd(kbs, name, waddr)
					defer bytespool.Put(nlist)
					listaddr := dp.blockWrite(haaddr, []byte(nlist))
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, listaddr)
					dp.diskRead(idbytest, rdbuf)
					return waddr, nil
				} else {
					return waddr, nil
				}
			}
		} else {
			if haaddr == 0 {
				//new name
				//掉电不乱数据保存过程
				//存储数据
				//记录数据地址到内存
				waddr := dp.blockWrite(0, data)
				if waddr != 0 {
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, waddr)
					dp.diskWrite(idbytest, rdbuf)
					return waddr, nil
				} else {
					return 0, e.E(8, "block write data error")
				}
			} else {
				/*
					块前4个bit数值情况:
					是否是流,是否是列表,未定义,未定义,3byte剩余表示数据长度;容量;上下块;
				*/
				if haaddr == addr {
					waddr := dp.blockWrite(haaddr, data)
					if waddr != 0 {
						if waddr != haaddr {
							dp.ReleaseAddr(haaddr)
							toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, waddr)
							dp.diskWrite(idbytest, rdbuf)
							return waddr, nil
						} else {
							return waddr, nil
						}
					} else {
						return 0, e.E(8, "block write data error")
					}
				} else {
					wdaaddr := dp.blockWrite(addr, data)
					//从haaddr获取名字
					tb := dp.OpenTable("", 0, haaddr)
					//组成两个名字的列表
					hana := tb.Name()
					tb.Close()
					list := "\n" + hana + "\t" + toolfunc.U64ToStr(haaddr) + "\n" + "\n" + name + "\t" + toolfunc.U64ToStr(wdaaddr)
					wlsaddr := dp.blockWrite(0, []byte(list))
					toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+1, 1)
					toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, wlsaddr)
					dp.diskWrite(idbytest, rdbuf)
					return wdaaddr, nil
				}
			}
		}
	}

	if name == "" && addr == 0 && id == 0 {
		waddr := dp.blockWrite(0, data)
		if waddr != 0 {
			return waddr, nil
		} else {
			return 0, e.E(8, "block write data error")
		}
	}

	return 0, e.E(8, "unknow error")
}

func (dp *Diskpoda) blockWrite(addr uint64, bs []byte) (waddrn uint64) {
	if lbs(bs) < (dp.Opt.MaxBlockSize)-3 {
		//存储修改极少或者每次全部不一样的数据小于4兆的数据
		tmp := bytespool.Get(int(dp.Opt.SectorSize))
		var head *DataBlockInfo
		if addr > 0 {
			//先读4kb,因为可能比较小
			/*
				块前4个bit数值情况:
				是否是流,是否是名字地址列表,未定义,未定义,3byte剩余表示数据长度;容量;上下块;
			*/
			tail := dp.ReadSectorTail(addr, tmp)
			//获取容量信息
			head = dp.blockHead(addr, tail)
		}
		sizn := dp.Opt.BlockSizeByteCnt + uint64(len(bs))
		addrn := dp.GetSpace(sizn)
		if addrn != 0 {
			toolfunc.BytesSetBits(tmp, 0, dp.Opt.AddrPrefixbitlen, 0b00)
			toolfunc.BytesSetBits(tmp, dp.Opt.AddrPrefixbitlen, dp.Opt.BlockSizeByteCnt*8, uint64(len(bs)))
			dp.diskWrite(addrn, tmp[:dp.Opt.BlockSizeByteCnt], bs)
			if head != nil {
				dp.PutSpace(head.size, addr)
			}
			bytespool.Put(tmp)
			return addrn
		} else {
			bytespool.Put(tmp)
			return 0
		}
	} else {
		sf := dp.OpenStream("", 0, addr, dp.Opt.UselessStreamBlockSize, 0)
		sf.WriteAll(bs)
		sf.Close()
	}
	return 0
}

func (dp *Diskpoda) GetData(name string, id uint32, addr uint64, outbuf []byte) (data []byte) {
	if addr > 0 {
		return dp.blockRead(addr, outbuf)
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
		retaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt)
		if retaddr == 0 {
			return nil
		} else {
			return dp.blockRead(addr, outbuf)
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
		e.P(3383, idbytest)
		dp.diskRead(idbytest, rdbuf)
		blist := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+1)
		haaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1)
		if blist == 1 {
			kbs := dp.blockRead(haaddr, nil)
			nameaddr := dp.nameAddrFind(kbs, name)
			if nameaddr == 0 {
				return nil
			} else {
				return dp.blockRead(nameaddr, outbuf)
			}
		} else {
			e.P("haaddr", haaddr)
			if haaddr == 0 {
				return nil
			} else {
				return dp.blockRead(haaddr, outbuf)
			}
		}
	}
	return nil
}

// bsbuf length < out size should release memory by bytespool.Put(outbs);
func (dp *Diskpoda) blockRead(addr uint64, bsbuf []byte) (bs []byte) {
	//先读4kb,因为可能比较小
	tmp := bytespool.Get(int(dp.Opt.SectorSize))
	tail := dp.ReadSectorTail(addr, tmp)
	head := dp.blockHead(addr, tail)
	if head.bstream == 0 {
		//获取容量信息
		if bsbuf == nil || cbs(bsbuf) < head.size {
			bs = bytespool.Get(int(head.size))
			if lbs(tail) >= dp.Opt.BlockSizeByteCnt+head.size {
				copy(bs, tail[dp.Opt.BlockSizeByteCnt:dp.Opt.BlockSizeByteCnt+head.size])
				return bs
			} else {
				copy(bs, tail[dp.Opt.BlockSizeByteCnt:])
				bscur := lbs(tail) - dp.Opt.BlockSizeByteCnt
				n, dre := dp.diskRead(addr+lbs(tail), bs[bscur:])
				if dre == nil {
					bs = bs[bscur : bscur+n]
					return bs
				} else {
					return nil
				}
			}
		} else {
			bsbuf = bsbuf[:head.size]
			if lbs(tail) >= dp.Opt.BlockSizeByteCnt+head.size {
				copy(bsbuf, tail[dp.Opt.BlockSizeByteCnt:dp.Opt.BlockSizeByteCnt+head.size])
				return bsbuf
			} else {
				copy(bsbuf, tail[dp.Opt.BlockSizeByteCnt:])
				bscur := lbs(tail) - dp.Opt.BlockSizeByteCnt
				n, dre := dp.diskRead(addr+lbs(tail), bsbuf[bscur:])
				if dre == nil {
					bsbuf = bsbuf[bscur : bscur+n]
					return bsbuf
				} else {
					return nil
				}
			}
		}
	} else {
		sf := dp.OpenStream("", 0, addr, dp.Opt.UselessStreamBlockSize, 0)
		outbs := sf.ReadAll(bsbuf)
		sf.Close()
		return outbs
	}
	return nil
}

// bsbuf length < out size should release memory by bytespool.Put(outbs);
func (dp *Diskpoda) ReleaseAddr(addr uint64) (err error) {
	//先读4kb,因为可能比较小
	tmp := bytespool.Get(int(dp.Opt.SectorSize))
	tail := dp.ReadSectorTail(addr, tmp)
	head := dp.blockHead(addr, tail)
	if head.bstream == 0 {
		//获取容量信息
		dp.PutSpace(dp.Opt.BlockSizeByteCnt+head.size, addr)
		return nil
	} else {
		sf := dp.OpenStream("", 0, addr, ^uint64(0), 0)
		sf.releaseAllSpae()
		sf.Close()
		return nil
	}
	return nil
}

func (ds *Diskpoda) nameAddrFind(buf []byte, name string) (nameaddr uint64) {
	se := "\n" + name + "\t"
	pos := bytes.Index(buf, []byte(se))
	addrbs := bytespool.Get(20)
	addrbs = addrbs[:0]
	for i := pos + len(se); i < len(buf); i += 1 {
		if buf[i] >= '0' && buf[i] <= '9' {
			addrbs = append(addrbs, buf[i])
		}
	}
	addr, addre := strconv.ParseUint(string(addrbs), 10, 64)
	bytespool.Put(addrbs)
	if addre == nil {
		return addr
	} else {
		return 0
	}
}

func (dp *Diskpoda) nameAddrListRead(h32 uint32) (ret string) {
	idbitst := dp.Opt.SectorSize*8 + 512*1024*1024*8 + uint64(h32)*dp.Opt.SizeBitCnt
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
	haaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt)
	if haaddr == 0 {
		return ""
	} else {
		/*
			块前4个bit数值情况:
			是否是流,是否压缩,是否是列表,是否有容量段,3byte剩余表示数据长度;容量;上下块;
		*/
		hada := bytespool.Get(4096)
		defer bytespool.Put(hada)
		dasize, dre := dp.diskRead(haaddr, hada) //先读4kb,再读剩余;
		if dre != nil {
			return ""
		}
		islist := toolfunc.BytesGetBits(hada, 1, 2)
		if islist == 1 {
			offset := uint64(3)
			return string(hada[offset : offset+dasize])
		} else {
			//dp.PutSpace(wid, pos)
			return ""
		}
	}
	return ret
}

// return list release by bytespool.Put;
func (dp *Diskpoda) nameAddrListAdd(nameadrdrsls []byte, name string, addr uint64) (ls []byte, oldaddr []byte) {
	se := []byte("\n" + name + "\t")
	pos := bytes.Index(nameadrdrsls, se)
	if pos != -1 {
		addrbs := bytespool.Get(20)
		defer bytespool.Put(addrbs)
		addrbs = addrbs[:0]
		for i := pos + len(se); i < len(nameadrdrsls); i += 1 {
			if nameadrdrsls[i] >= '0' && nameadrdrsls[i] <= '9' {
				addrbs = append(addrbs, nameadrdrsls[i])
			}
		}
		return toolfunc.JoinBytes(nameadrdrsls[:pos], se, []byte(toolfunc.U64ToStr(addr)), nameadrdrsls[pos+len(se)+len(addrbs):]), addrbs
	} else {
		return toolfunc.JoinBytes(nameadrdrsls, se, []byte(toolfunc.U64ToStr(addr))), nil
	}
}

// return list release by bytespool.Put;
func (dp *Diskpoda) nameAddrListRemove(nameadrdrsls []byte, name string) (nlist, oldaddr []byte) {
	se := []byte("\n" + name + "\t")
	pos := bytes.Index(nameadrdrsls, se)
	if pos != -1 {
		addrbs := bytespool.Get(20)
		defer bytespool.Put(addrbs)
		addrbs = addrbs[:0]
		for i := pos + len(se); i < len(nameadrdrsls); i += 1 {
			if nameadrdrsls[i] >= '0' && nameadrdrsls[i] <= '9' {
				addrbs = append(addrbs, nameadrdrsls[i])
			}
		}
		return toolfunc.JoinBytes(nameadrdrsls[:pos], nameadrdrsls[pos+len(se)+len(addrbs):]), nameadrdrsls[pos+len(se) : pos+len(se)+len(addrbs)]
	}
	return toolfunc.JoinBytes(nameadrdrsls), nil
}

func (dp *Diskpoda) DeleteData(name string, id, addr uint64) (er error) {
	//全部提供每个地方都会删除
	baddrrel := false
	if addr > 0 {
		dp.ReleaseAddr(addr)
		baddrrel = true
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
		retaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt)
		if baddrrel == false {
			dp.ReleaseAddr(addr)
		} else if addr > 0 && retaddr != addr {
			panic(e.E(4, "algorithm error"))
		}
		toolfunc.BytesSetBits(rdbuf, idbitst%8, idbitst%8+dp.Opt.SizeBitCnt, 0)
		dp.diskWrite(idbytest, rdbuf)
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
		e.P(3383, idbytest)
		dp.diskRead(idbytest, rdbuf)
		blist := toolfunc.BytesGetBits(rdbuf, idbitst%8, idbitst%8+1)
		haaddr := toolfunc.BytesGetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1)
		if blist == 1 {
			kbs := dp.blockRead(haaddr, nil)
			nls, oaddr := dp.nameAddrListRemove(kbs, name)
			if baddrrel == false {
				dp.ReleaseAddr(toolfunc.Uint64FromStr(string(oaddr)))
			} else if addr > 0 && toolfunc.Uint64FromStr(string(oaddr)) != addr {
				panic(e.E(4, "algorithm error"))
			}
			waddr := dp.blockWrite(haaddr, nls)
			if waddr == haaddr {
				return nil
			} else {
				toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, waddr)
				dp.diskWrite(haaddr, rdbuf)
				return nil
			}
		} else {
			if baddrrel == false {
				dp.ReleaseAddr(haaddr)
			} else if addr > 0 && haaddr != addr {
				panic(e.E(4, "algorithm error"))
			}
			toolfunc.BytesSetBits(rdbuf, idbitst%8+1, idbitst%8+dp.Opt.SizeBitCnt+1, 0)
			dp.diskWrite(haaddr, rdbuf)
			return nil
		}
	}
	return nil
}

func (dp *Diskpoda) AllocId() (id uint32) {
	rdcnt := 0
	for true {
		if dp.IdBlocks[dp.Opt.IdSegCur] == nil {
			dp.IdBlocks[dp.Opt.IdSegCur] = bytespool.Get(512 * 1024)
			dp.diskRead(dp.Opt.SectorSize+dp.Opt.IdSegCur*512*1024, dp.IdBlocks[dp.Opt.IdSegCur])
		}
		for i := 0; i < len(dp.IdBlocks[dp.Opt.IdSegCur])/8; i += 1 {
			if binary.BigEndian.Uint64(dp.IdBlocks[dp.Opt.IdSegCur][i*8:i*8+8]) != ^uint64(0) {
				for i4 := uint64(0); i4 < 64; i4 += 1 {
					if toolfunc.BytesGet1Bit(dp.IdBlocks[dp.Opt.IdSegCur][i*8:i*8+8], i4) == 0 {
						toolfunc.BytesSet1Bit(dp.IdBlocks[dp.Opt.IdSegCur][i*8:i*8+8], i4, 1)
						return uint32(dp.Opt.IdSegCur*8 + uint64(i)*8*8 + i4)
					}
				}
				panic("error")
			}
		}
		rdcnt += 1
		dp.Opt.IdSegCur += 1
		if dp.Opt.IdSegCur == 1024 {
			dp.Opt.IdSegCur = 0
		}
		if rdcnt == 1024 {
			break
		}
	}
	return 0
}

func (dp *Diskpoda) blockHead(addr uint64, bs []byte) (bk *DataBlockInfo) {
	bk = &DataBlockInfo{}
	bk.addr = addr
	bk.bstream = toolfunc.BytesGetBits(bs, 0, 1)
	bk.bcompress = toolfunc.BytesGetBits(bs, 1, 2)
	if bk.bstream == 0 {
		bk.size = toolfunc.BytesGetBits(bs, dp.Opt.AddrPrefixbitlen, dp.Opt.BlockSizeByteCnt*8)
	}
	return bk
}

func (dp *Diskpoda) streamBlockHead(addr uint64, bs []byte) (bk *StreamBlockInfo) {
	bk = &StreamBlockInfo{}
	bk.addr = addr
	bk.bstream = toolfunc.BytesGetBits(bs, 0, 1)
	bk.bcompress = toolfunc.BytesGetBits(bs, 1, 2)
	bk.pre = toolfunc.BytesGetBits(bs, dp.Opt.AddrPrefixbitlen, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen)
	bk.next = toolfunc.BytesGetBits(bs, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen*2)
	return bk
}

func (dp *Diskpoda) Sync() error {
	dp.initFirstSectorOption()
	dp.writeNumnumsHead()
	return nil
}

func (dp *Diskpoda) Close() error {
	if dp.diskf != 0 {
		ClosePartition(dp.diskf)
		dp.diskf = 0
	}
	return nil
}

//for tables value

func (dp *Diskpoda) PutTable(name string, id uint32, addr uint64, tb *Table) (retaddr uint64, er Error) {
	return 0, NewError("Set fail")
}

func (dp *Diskpoda) GetTable(name string, id uint32, addr uint64) (tb *Table) {
	return nil
}

func (dp *Diskpoda) CondSet(condses, setses *Table) Error {
	return NewError("Set fail")
}

func (dp *Diskpoda) CondGet(condses, getses *Table) (tb []*Table) {
	return nil
}

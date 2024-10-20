package diskpoda

import (
	"bytespool"
	"e"

	"linbo.ga/toolfunc"
)

type SpaceBlockInfo struct {
	blockpos uint64
	block    []byte
	addrcur  uint64
	bmod     bool
}

/*
for orderfile dp.Opt.Numlen+dp.Opt.Numsilen=64  default:19+45  bit
gor orderfilepeta dp.Opt.Numlen+dp.Opt.Numsilen=80 default:25+55  bit

for orderfile Nums sequence: 48 bit 6byte
gor orderfilepeta Nums sequence: 56  bit  7byte
databitl:可以取的值:40(256g db),48(<256tb fb),56(1pb db),64(16 eksabajt; orderilepeta max can be 4zetabajt)
*/

func (dp *Diskpoda) PutSpace(lendata uint64, addr uint64) error {
	lendata = dp.DiskStepSize(uint64(lendata))
	sbi, b := dp.Num_block[uint32(lendata)]
	if b {
		if (sbi.addrcur+1)*dp.Opt.SizeBitCnt <= dp.Opt.NumnumsBlockSize*8-dp.Opt.TwoAddrbytelen*8 {
			toolfunc.BytesSetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+sbi.addrcur*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+(sbi.addrcur+1)*dp.Opt.SizeBitCnt, addr)
			sbi.addrcur += 1
			return nil
		} else {
			bki := dp.streamBlockHead(sbi.blockpos, sbi.block)
			if bki.next != 0 {
				if sbi.bmod {
					dp.diskWrite(sbi.blockpos, sbi.block)
				}
				sbi.blockpos = bki.next
				sbi.addrcur = 0
				toolfunc.BytesSetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+sbi.addrcur*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+(sbi.addrcur+1)*dp.Opt.SizeBitCnt, addr)
				sbi.addrcur += 1
				sbi.bmod = true
				return nil
			} else {
				naddr := dp.DiskAllocSpace(dp.Opt.NumnumsBlockSize)
				toolfunc.BytesSetBits(sbi.block, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen*2, naddr)
				dp.diskWrite(sbi.blockpos, sbi.block)
				toolfunc.BytesSetBits(sbi.block, 0, 1, 1)
				toolfunc.BytesSetBits(sbi.block, dp.Opt.AddrPrefixbitlen, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen, sbi.blockpos)
				toolfunc.BytesSetBits(sbi.block, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen, dp.Opt.AddrPrefixbitlen+dp.Opt.Addrbitlen*2, 0)
				sbi.blockpos = naddr
				sbi.addrcur = 0
				toolfunc.BytesSetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+sbi.addrcur*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+(sbi.addrcur+1)*dp.Opt.SizeBitCnt, addr)
				sbi.addrcur += 1
				sbi.bmod = true
				return nil
			}
		}
	} else {
		sbi := &SpaceBlockInfo{blockpos: 0, block: nil, addrcur: 0}
		addr := dp.DiskAllocSpace(dp.Opt.NumnumsBlockSize)
		sbi.blockpos = addr
		sbi.block = bytespool.Gen(int(dp.Opt.NumnumsBlockSize), int(dp.Opt.NumnumsBlockSize))
		sbi.addrcur = 0
		toolfunc.BytesSetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+sbi.addrcur*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+(sbi.addrcur+1)*dp.Opt.SizeBitCnt, addr)
		sbi.addrcur += 1
		sbi.bmod = true
		dp.Num_block[uint32(lendata)] = sbi
		return nil
	}
}

// fail 0;
func (dp *Diskpoda) GetSpace(lendata uint64) (addr uint64) {
	lendata = dp.DiskStepSize(uint64(lendata))
	sbi, b := dp.Num_block[uint32(lendata)]
	if b {
		if sbi.addrcur > 0 {
			addr = toolfunc.BytesGetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+(sbi.addrcur-1)*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+sbi.addrcur*dp.Opt.SizeBitCnt)
			sbi.addrcur -= 1
			return addr
		} else {
			bki := dp.streamBlockHead(sbi.blockpos, sbi.block)
			if bki.pre != 0 {
				if sbi.bmod {
					dp.diskWrite(sbi.blockpos, sbi.block)
				}
				dp.diskRead(bki.pre, sbi.block)
				sbi.blockpos = bki.pre
				sbi.addrcur = (dp.Opt.NumnumsBlockSize*8 - dp.Opt.TwoAddrbytelen*8) / dp.Opt.SizeBitCnt
				addr = toolfunc.BytesGetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+(sbi.addrcur-1)*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+sbi.addrcur*dp.Opt.SizeBitCnt)
				sbi.addrcur -= 1
				sbi.bmod = true
				return addr
			} else {
				addr := dp.DiskAllocSpace(dp.Opt.NumnumsBlockSize)
				sbi.blockpos = addr
				sbi.addrcur = 0
				n := uint64(dp.stepsiz_nforsector[uint32(lendata)])
				saddr := dp.DiskAllocSpace(uint64(lendata) * n)
				for i := uint64(0); i < n-1; i += 1 {
					toolfunc.BytesSetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+i*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+(i+1)*dp.Opt.SizeBitCnt, saddr+i*uint64(lendata))
					sbi.addrcur += 1
				}
				sbi.bmod = true
				return saddr + (n-1)*uint64(lendata)
			}
		}
	} else {
		sbi := &SpaceBlockInfo{blockpos: 0, block: nil, addrcur: 0}
		addr := dp.DiskAllocSpace(dp.Opt.NumnumsBlockSize)
		sbi.blockpos = addr
		sbi.block = bytespool.Gen(int(dp.Opt.NumnumsBlockSize), int(dp.Opt.NumnumsBlockSize))
		sbi.addrcur = 0
		n := uint64(dp.stepsiz_nforsector[uint32(lendata)])
		saddr := dp.DiskAllocSpace(uint64(lendata) * n)
		for i := uint64(0); i < n-1; i += 1 {
			e.P(i, n, dp.Opt.SizeBitCnt, lendata)
			toolfunc.BytesSetBits(sbi.block, dp.Opt.TwoAddrbytelen*8+i*dp.Opt.SizeBitCnt, dp.Opt.TwoAddrbytelen*8+(i+1)*dp.Opt.SizeBitCnt, saddr+i*uint64(lendata))
			sbi.addrcur += 1
		}
		sbi.bmod = true
		dp.Num_block[uint32(lendata)] = sbi
		return saddr + (n-1)*uint64(lendata)
	}
}

func (dp *Diskpoda) DiskStepSize(size uint64) uint64 {
	sizbitcnt := toolfunc.GetSizeBitCnt(size)
	switch sizbitcnt {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13:
		if size%8 == 0 {
			return size
		} else {
			return size + 8 - size%8
		}
	default:
		modval := uint64(1 << (sizbitcnt - 10))
		if modval > dp.Opt.MaxModelVal {
			modval = dp.Opt.MaxModelVal
		}
		if size == 0 || modval == 0 {
			e.P(size, sizbitcnt, size, modval, dp.Opt.MaxModelVal)
		}
		if size%modval == 0 {
			return size
		} else {
			return size + modval - size%modval
		}
	}
}

func (dp *Diskpoda) DiskAllocSpace(size uint64) (id uint64) {
	newpo := dp.Opt.FreeSpaceCur
	dp.Opt.FreeSpaceCur += uint64(size)
	e.P(dp.Opt.FreeSpaceCur)
	return uint64(newpo)
}

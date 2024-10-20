package diskpoda

/*

#if defined(WIN32)
#include <windows.h>

unsigned long long OpenPartition(char *path)
{
	HANDLE device = NULL;
    device = CreateFile(path,   //"\\\\.\\H:",    // Drive to open
        GENERIC_READ|GENERIC_WRITE,           // Access mode
        FILE_SHARE_READ,        // Share Mode
        NULL,                   // Security Descriptor
        OPEN_ALWAYS,          // How to create
        0,                      // File attributes
        NULL);                  // Handle to template
	if((unsigned long long)(device)==18446744073709551615){
		return 0;
	}else{
		return (unsigned long long)(device);
	}
}

unsigned long long ClosePartition(unsigned long long device)
{
	CloseHandle(device);
}

unsigned long long Sync(unsigned long long device)
{
	FlushFileBuffers(device);
	return 0;
}

int ReadSector(unsigned long long device,unsigned long long secpos ,void* buf,int readsecsize)
{
    if(device != NULL)
    {
    		LONG hipt=secpos>>32;
        DWORD spr=SetFilePointer (device, secpos, &hipt, FILE_BEGIN);
        //printf("wsec %llu %llu\n",secpos, spr);
        DWORD bytesRead=0;
        if (!ReadFile(device, buf, readsecsize, &bytesRead, NULL))
        {
            return 0;
        }
        else
        {
            return bytesRead;
        }
    }
    return 0;
}

int WriteSector(unsigned long long device,unsigned long long secpos ,void* buf,int writesecsize)
{
    if(device != NULL)
    {
    		LONG hipt=secpos>>32;
        DWORD spr=SetFilePointer (device, secpos, &hipt, FILE_BEGIN);
        //printf("wsec %llu %llu\n",secpos, spr);
        DWORD bytesWrite=0;
        if (!WriteFile(device, buf, writesecsize, &bytesWrite, NULL))
        {
            return 0;
        }
        else
        {
            return bytesWrite;
        }
    }
    return 0;
}
#elif defined(__FreeBSD__) || defined(__linux__)
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


unsigned long long OpenPartition(char *path)
{
	if(strstr(path,"/dev/")!=0{
		int fd=open(path, O_RDWR | O_DIRECT);
		return (unsigned long long)fd;
	}else{
		int fd=open(path, O_RDWR | O_CREATE);
		return (unsigned long long)fd;
	}
}

void ClosePartition(unsigned long long device)
{
	close(device);
}


unsigned long long Sync(unsigned long long device)
{
	fsync(device);
	return 0;
}


int ReadSector(unsigned long long device,unsigned long long secpos ,void* buf,int readsecsize)
{
    if(device != 0)
    {
    	lseek(device, secpos, SEEK_SET);
		return read(device,buf,readsecsize);//<0 is error;
    }
    return 0;
}


int WriteSector(unsigned long long device,unsigned long long secpos ,void* buf,int writesecsize)
{
    if(device != 0)
    {
    	lseek(device, secpos, SEEK_SET);
		return write(device,buf,writesecsize);//<0 is error;
    }
    return 0;
}


#endif


*/
import "C"
import (
	"bytespool"
	"e"
	"unsafe"
)

func OpenPartition(path string) uint64 {
	return uint64(C.OpenPartition(C.CString(path)))
}

func ClosePartition(parth uint64) {
	C.ClosePartition(C.ulonglong(parth))
}

func (dp *Diskpoda) ReadSector(secpos uint64, bs []byte) uint64 {
	secrdn := uint64(C.ReadSector(C.ulonglong(dp.diskf), C.ulonglong(secpos), unsafe.Pointer(&bs[0]), C.int(len(bs))))
	//e.P("dp.ReadSector", secpos, secrdn, len(bs))
	if secrdn != lbs(bs) || lbs(bs)%dp.Opt.SectorSize != 0 || secrdn%dp.Opt.SectorSize != 0 {
		e.P("dp.ReadSector", secpos, secrdn, len(bs))
		if dp.bdev == false {
			return lbs(bs)
		}
		panic("dp.ReadSector data error")
		return 0
	}
	return secrdn
}

// outsectabuf is nil use bytespool.Put(outbs) to release memory;
func (dp *Diskpoda) ReadSectorTail(addr uint64, outsectabuf []byte) (outsecta []byte) {
	bs := bytespool.Get(int(dp.Opt.SectorSize))
	secpos := addr - addr%dp.Opt.SectorSize
	secrdn := uint64(C.ReadSector(C.ulonglong(dp.diskf), C.ulonglong(secpos), unsafe.Pointer(&bs[0]), C.int(len(bs))))
	if secrdn != lbs(bs) || lbs(bs)%dp.Opt.SectorSize != 0 || secrdn%dp.Opt.SectorSize != 0 {
		e.P("dp.ReadSector", secpos, secrdn, len(bs))
		panic("dp.ReadSector data error")
		return nil
	}
	if outsectabuf == nil || cbs(outsectabuf) < dp.Opt.SectorSize-addr%dp.Opt.SectorSize {
		outsectabuf = bytespool.Get(int(dp.Opt.SectorSize - addr%dp.Opt.SectorSize))
	} else {
		outsectabuf = outsectabuf[:dp.Opt.SectorSize-addr%dp.Opt.SectorSize]
	}
	copy(outsectabuf, bs[addr%dp.Opt.SectorSize:])
	outsectabuf = outsectabuf[:dp.Opt.SectorSize-addr%dp.Opt.SectorSize]
	bytespool.Put(bs)
	return outsectabuf
}

func (dp *Diskpoda) WriteSector(secpos uint64, bs []byte) uint64 {
	if len(bs) == 0 {
		return 0
	}
	secwn := uint64(C.WriteSector(C.ulonglong(dp.diskf), C.ulonglong(secpos), unsafe.Pointer(&bs[0]), C.int(len(bs))))
	e.P("dp.WriteSector", dp.diskf, secpos, secwn, len(bs))
	if secwn != lbs(bs) || lbs(bs)%dp.Opt.SectorSize != 0 || secwn%dp.Opt.SectorSize != 0 {
		e.P("dp.WriteSector", dp, secwn != lbs(bs), lbs(bs)%dp.Opt.SectorSize != 0, secwn%dp.Opt.SectorSize != 0, secpos, secwn, len(bs))
		panic("dp.WriteSector data error")
		return 0
	}
	return secwn
}

func (dp *Diskpoda) diskfSync() error {
	C.Sync(C.ulonglong(dp.diskf))
	return nil
}

func (dp *Diskpoda) diskRead(pos uint64, outbs []byte) (n uint64, er error) {
	//e.P("diskRead", pos, len(outbs))
	if pos%dp.Opt.SectorSize == 0 {
		obkssiz := lbs(outbs) - lbs(outbs)%dp.Opt.SectorSize
		if obkssiz > 0 {
			dp.ReadSector(uint64(pos), outbs[0:obkssiz])
		}
		if lbs(outbs)%dp.Opt.SectorSize != 0 {
			bu := bytespool.Get(int(dp.Opt.SectorSize))
			dp.ReadSector(pos+obkssiz, bu)
			copy(outbs[obkssiz:], bu[:lbs(outbs)%dp.Opt.SectorSize])
			bytespool.Put(bu)
		}
		return lbs(outbs), nil
	} else {
		bu := bytespool.Get(int(dp.Opt.SectorSize))
		firstsec := pos - pos%dp.Opt.SectorSize
		dp.ReadSector(uint64(firstsec), bu)
		endpo := pos + uint64(len(outbs))
		if endpo <= firstsec+dp.Opt.SectorSize {
			endpo2 := endpo % dp.Opt.SectorSize
			if endpo == firstsec+dp.Opt.SectorSize {
				endpo2 = dp.Opt.SectorSize
			}
			copy(outbs, bu[pos%dp.Opt.SectorSize:endpo2])
			bytespool.Put(bu)
			//e.P(bu)
			return uint64(len(outbs)), nil
		}
		copy(outbs, bu[pos%dp.Opt.SectorSize:])
		firstsec += dp.Opt.SectorSize
		bsst := dp.Opt.SectorSize - pos%dp.Opt.SectorSize
		raiml := uint64(len(outbs)) - (dp.Opt.SectorSize - pos%dp.Opt.SectorSize)
		nsec := (raiml) / dp.Opt.SectorSize
		//e.P(pos, pos%2, bsst, len(outbs), raiml, nsec, firstsec)
		if nsec > 0 {
			dp.ReadSector(uint64(firstsec), outbs[bsst:bsst+nsec*dp.Opt.SectorSize])
		}
		firstsec += nsec * dp.Opt.SectorSize
		if raiml%dp.Opt.SectorSize != 0 {
			dp.ReadSector(uint64(firstsec), bu)
			copy(outbs[bsst+nsec*dp.Opt.SectorSize:], bu[:raiml%dp.Opt.SectorSize])
			firstsec += raiml % dp.Opt.SectorSize
		}
		bytespool.Put(bu)
		return firstsec - pos, er
	}
}

func (dp *Diskpoda) diskWrite(pos uint64, bsls ...[]byte) (er error) {
	if pos%dp.Opt.SectorSize == 0 && len(bsls) == 1 {
		bkssiz := lbs(bsls[0]) - lbs(bsls[0])%dp.Opt.SectorSize
		if bkssiz > 0 {
			dp.WriteSector(pos, bsls[0][:bkssiz])
		}
		if lbs(bsls[0])%dp.Opt.SectorSize != 0 {
			bu := bytespool.Gen(int(dp.Opt.SectorSize), int(dp.Opt.SectorSize))
			dp.diskRead(pos+bkssiz, bu)
			for i6 := 0; i6 < int(lbs(bsls[0])%dp.Opt.SectorSize); i6 += 1 {
				bu[i6] = bsls[0][bkssiz+uint64(i6)]
			}
			dp.WriteSector(pos+bkssiz, bu)
			bytespool.Put(bu)
		}
		return nil
	} else {
		bu := bytespool.Gen(int(dp.Opt.SectorSize), int(dp.Opt.SectorSize))
		dp.diskRead(pos-pos%dp.Opt.SectorSize, bu)
		bu = bu[:pos%dp.Opt.SectorSize]
		e.P("diskWrite", pos, pos%dp.Opt.SectorSize, "bu len", lbs(bu), len(bsls), bsls)
		cnt := 0
		for _, bs := range bsls {
			for i1 := uint64(0); i1 < lbs(bs); i1 += 1 {
				bu = append(bu, bs[i1])
				cnt += 1
				if uint64(len(bu)) == dp.Opt.SectorSize {
					dp.WriteSector(pos-pos%dp.Opt.SectorSize, bu)
					npos := pos - pos%dp.Opt.SectorSize + dp.Opt.SectorSize
					for i4 := i1 + 1; i4 < lbs(bs); i4 += 512 * 1024 {
						wcnt := uint64(512 * 1024)
						if i4+wcnt > lbs(bs) {
							wcnt = lbs(bs) - i4
						}
						e.P("npos", npos, "wcnt", wcnt)
						if wcnt%dp.Opt.SectorSize != 0 {
							presecsiz := (wcnt / dp.Opt.SectorSize) * dp.Opt.SectorSize
							e.P("npos", npos, "i4", i4, "wcnt", wcnt)
							dp.WriteSector(npos, bs[i4:i4+presecsiz])
							dp.diskRead(npos+presecsiz, bu)
							for i6 := 0; i6 < int(wcnt%dp.Opt.SectorSize); i6 += 1 {
								bu[i6] = bs[i4+presecsiz+uint64(i6)]
							}
							dp.WriteSector(npos+presecsiz, bu)
						} else {
							dp.WriteSector(npos, bs[i4:i4+wcnt])
						}
						npos += uint64(wcnt)
					}
					bytespool.Put(bu)
					return nil
				}
			}
		}
		bu = bu[:dp.Opt.SectorSize]
		dp.WriteSector(pos-pos%dp.Opt.SectorSize, bu)
		bytespool.Put(bu)
		return nil
	}
}

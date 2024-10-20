package diskpoda

import "errors"

/*
walilang 瓦乐语言--脚本语言--trelidb内部脚本语言
数据库存储表，表下面还可以有表，还可以关联到其他键下表；
数据库内数据都可以计算验证;
名字下可以表级联索引，够成索引网；


一个系统会有那些错误：
找不到数据，或者数据不符合，数据异常，数据错误，值错误
算法出来的结果有误
即将执行的动作不符合逻辑
检测到不可能的情
不可能结果
无结果
无处理代码
系统环境错误：
	内存错误
	磁盘错误
	设备错误
	CPU错误
	线断开
	信号丢失

错误按照处理方式有：
	立即处理
	panic方式处理
	处理错误的壳函数运行代码行





*/

// 拥有属性：二维表，列表，文件，句子，行数据，列数据,代码段，可输入输出代码段，
// 表数据存储：段落式，流式，标记式，段落由长度数据组成，段落可以是一条行数据或则列数据。
// 能表示的表是离散的，有的表头有行数据，有的只有表头与值；
// 表数据修改，表的首地址是必须不变的；
type Table struct {
	dp     *Diskpoda
	name   string
	id     uint64 //but 32 bit;
	addr   uint64 //
	tbdata []byte

	oid uint64
	er  string

	typeid uint32

	vint    int
	vuint   uint
	vint8   int8
	vuint8  uint8
	vint16  int16
	vuint16 uint16
	vint32  int32
	vuint32 uint32
	vint64  int64
	vuint64 uint64
	vltbyte []byte
	vstring string

	code string
}

//类型可以远超下面已经定义的，其他类型可以定义在数据库，与go代码写的一样，甚至可以重载go定义的；
/*
依托数据库语言：
全部用表格表示
空格隔开词根，tab表示行制表符，逗号表示同层名依次字列举， （.）表示属性或则方法调用，of表示属性之，
各种东西都是表，大表嵌入小表
表的文本表示就是带indent的文本块
if img is 8:
	kkddd
	ddduu due7dhhvf
	w7du eww7d ffrr
else if img<77:
	djjdjjd
	dddd
else:
	ide33e

for img is 8:
	ded ed  ed ed e d
//不需要参数，直接修改初始化后的环境，不需要返回，直接引用函数环境：
//函数可以在类下，也可以在对象下，可以在基础类型下，在类下的函数可以用类名调用，有的必须初始化类后调用
//
function dde:
	中洒33//初始化语句
	djjd
	code:
	dhhddh
	ddddddd
	ddddd
	return://可以定义也可以不定义，不定就是整个环境中数据都可以是返回值，定义了就只能取定义的；



dde:
	ddjjd=777d
	:
	uu=ddd
	kk=uuu

class ddue:
	ddd
	fffffff
=左边表示被赋值名，没有就自动创建，右边类名 参数，或则参数 类名，或则函数调用；
aaa.func:
	//创造或者更新函数的运行环境
	套上d
	dddhhdhhd
	://取出结果
	uuddddd
	ddddddd
333.sin.cos.tan
bb=333.sin.cos.tan//必须与返回定义对应
馻=33.sin&//表示返回环境
for k>333:
	u7eeuud
	eedffgt

“djjd”.uuddj:ssd=22,fff=222:func.ddd:yyyydd=333,ddd=2233:func.ssss
“djjd”.uuddj:ssd=22,fff=int.randn:333:bb:func.ddd:yyyydd=333,ddd=2233:func.ssss //不好分句,参数还有函数调用的必须写成indent形式(树表式)
//上面调用树表式写法如下，这种写法不要复杂解析算法
“djjd”.uuddj:
	ssd=22
	fff=int.randn:333:bb
	end:（也可以直接冒号）
	func.ddd:yyyydd=333,ddd=2233:func.ssss
冒号结尾的特殊标签：
	code:
	end:


复杂网络表示：
	通常是树形的，网型表示用于复杂数据
	多个树上名字指向一个名字，则认为是网络



数据库除了上面条目当文件与数活功能外，还应该包含调试测试功能：
rltb:=db.Execute(code)//定义了return返回return,否则返回环境变量；
如果有个cmd,要有功能:
	变量定义
	修改数据库的脚本功能
	计算功能
	函数定义修改存储(函数对象能取出代码)
	对象定义修改存储
	（所有能存储变量能存能读）

一个依托数据库的脚本语言吗：
	硬件只是转换成描述
	在数据库内部全部是描述算法
	就要运用这些推导下一步


golang语言有包,方法成套在里面，那么这个脚本呢？：
	可以有包，包名函数写起调用：
这个脚本语言解析：
	按行解析，行前带段距空（段落距离空白，英语indent）
	无括号，无方括号，无大括号，且无各种括号嵌套，解析运行速度快。
这个脚步语言的基础：
	参照golang语言
	文件系统 要
	网络 要
	界面 以后要
	注解功能 要
	变量定义 要
	加减乘除 要
	山脚函数 要
	随机数 庶
	多重继承  defname=name-to-init-every-class class1,lass2
	模板 能适应多种类型的一个或者一套方法 就是附加了个名子而已，应该跟多重继承一样。
	运行的是语句流，处理的是描述，性能应该可以
	运行的是语句流，处理的是直接数据，性能应该差很多，怎么办？：
		脚本主可以生成golang程序，管理golang程序，以提高处理速度；
	要用语义模板句子，以免歧义，到底正向解析还是反向解析，要是明确的；

脚本文本结构与解析：
	是否需要整篇代码预解析 不太可能有内存装不下的一篇代码 所以要完整解析再运行（？边解析代码行边运行有什么问题，好像也没有问题，需要推算一步运行一步时用）
	各种运行都有检测器，检测通过了，才执行运行
	行运行可回退

要有输入描述，要用于计算的名子都要有最大程度的定义

是否支持符号计算 在不破坏用优美文字情况下可以有
数据类型自带最优默认转换 似的

*/

type Error *Table
type List = *Table
type File = *Table
type Row = *Table
type Collum = *Table
type Object = *Table
type Any = *Table
type Vector = *Table
type HVector = *Table
type VVector = *Table
type Matrix = *Table
type Code = *Table
type Function = *Table
type Enviroment = *Table
type Sentence = *Table
type Setorld = *Table //sentence element;part of sentence;
type Setoret = *Table //entorld's word;
type Pipe = *Table
type IdSet = *Table
type Range = *Table
type RangeSet = *Table
type Int = *Table
type Uint = *Table
type Int8 = *Table
type Uint8 = *Table
type Int16 = *Table
type Uint16 = *Table
type Int32 = *Table
type Uint32 = *Table
type Int64 = *Table
type Uint64 = *Table
type String = *Table
type Float32 = *Table
type Float64 = *Table
type Bool = *Table
type Bytes = *Table
type Ints = *Table
type Uints = *Table
type Int8s = *Table
type Uint8s = *Table
type Int16s = *Table
type Uint16s = *Table
type Int32s = *Table
type Uint32s = *Table
type Int64s = *Table
type Uint64s = *Table
type Strings = *Table
type Bools = *Table
type Float32s = *Table
type Float64s = *Table
type Tree = *Table
type Graph = *Table
type KeyValue = *Table
type Length = *Table
type Width = *Table
type Height = *Table
type Temperature = *Table
type Dampness = *Table
type Cubage = *Table
type Area = *Table
type Speed = *Table
type Energy = *Table
type Percentage = *Table
type Rmb = *Table
type Dollar = *Table
type Weight = *Table
type Time = *Table
type Force = *Table
type Current = *Table
type Voltage = *Table
type Watt = *Table
type Quantity = *Table
type Number = *Table
type Coordinate = *Table
type Placename = *Table
type DecimalNumber = *Table
type BinaryNumber = *Table
type Integer = *Table
type StateOfMatter = *Table
type Color = *Table
type Density = *Table
type Intensity = *Table
type ElectricEnerger = *Table
type BatteryCapacity = *Table
type ElectricQuantity = *Table
type Equation = *Table
type Bit = *Table
type Image = *Table
type Sound = *Table
type Video = *Table
type SpaceModel = *Table
type TreeMap = *Table

const (
	ERROR = iota
	LIST
	FILE
	ROW
	COLLUM
	OBJECT
	ANY
	VECTOR
	MATRIX
	CODE
	FUNCTION
	ENVIROMENT
	SENTENCE
	SENTORLD //SENTENCE ELEMENT;PART OF SENTENCE;
	SITORD   //SENTORLD'S WORD;
	PIPE

	INT
	UINT
	INT8
	UINT8
	INT16
	UINT16
	INT32
	UINT32
	INT64
	UINT64
	LISTBYTE
	STRING
)

func NewValue(v any) Any {
	tb := &Table{}
	switch v.(type) {
	case int:
		tb.vint = v.(int)
	case uint:
		tb.vuint = v.(uint)
	case int8:
		tb.vint8 = v.(int8)
	case uint8:
		tb.vuint8 = v.(uint8)
	case int16:
		tb.vint16 = v.(int16)
	case uint16:
		tb.vuint16 = v.(uint16)
	case int32:
		tb.vint32 = v.(int32)
	case uint32:
		tb.vuint32 = v.(uint32)
	case int64:
		tb.vint64 = v.(int64)
	case uint64:
		tb.vuint64 = v.(uint64)
	case []byte:
		tb.vltbyte = v.([]byte)
	case string:
		tb.vstring = v.(string)
	default:
	}
	return tb
}

func NewError(s string) Error {
	e := Error(&Table{})
	e.er = s
	return e
}

func NewVector() Vector {
	e := Vector(&Table{})
	return e
}

func NewMatrix() Matrix {
	e := Matrix(&Table{})
	return e
}

func NewFile(s string) Error {
	e := File(&Table{})
	e.er = s
	return e
}

func NewList() Vector {
	e := List(&Table{})
	return e
}

func NewObject() Object {
	e := Object(&Table{})
	return e
}

func NewRow() Row {
	e := Row(&Table{})
	return e
}

func NewCollum() Collum {
	e := Collum(&Table{})
	return e
}

func NewCode() Code {
	e := Code(&Table{})
	return e
}

func NewFunction() Code {
	e := Function(&Table{})
	return e
}

func NewEnviroment() Enviroment {
	e := Enviroment(&Table{})
	return e
}

func NewSentence() Sentence {
	e := Sentence(&Table{})
	return e
}

func (dp *Diskpoda) OpenTable(name string, id uint64, addr uint64) (tb *Table) {
	tb = &Table{dp: dp, name: name, id: id, addr: addr}
	return tb
}

func (tb *Table) Name() string {
	return tb.name
}

func (tb *Table) AddRow(row map[string]string) Error {
	return NewError("add row fail")
}

func (tb *Table) AddCollum(key string, val any) Error {
	return NewError("add collum fail")
}

func (tb *Table) SeletRow(rules map[string]string, sorthead string) (rs []*Row) {
	return nil
}

func (tb *Table) SeletCollum(rules map[string]string) (rs []*Collum) {
	return nil
}

func (tb *Table) ModifyRow(rules map[string]string, row map[string]string) Error {
	return NewError("modify row error")
}

func (tb *Table) ModifyCollum(rules map[string]string, colname string, dta []byte) Error {
	return NewError("modify collum error")
}

func (tb *Table) DeleteRow(rules map[string]string, rlcnt int) Error {
	return NewError("delete row error")
}

func (tb *Table) DeleteCollum(col string) Error {
	return NewError("delete collum error")
}

func (tb *Table) AttrSet(key string, val Any) Error {
	return NewError("attr set error")
}

func (tb *Table) AttrGet(key string) (val Any) {
	return nil
}

func (tb *Table) AttriDelete(row map[string]string) Error {
	return NewError("attr delete error")
}

func (tb *Table) GetParents() (parents []*Table) {
	return nil
}

func (tb *Table) GetChildren() (children []*Table) {
	return nil
}

func (tb *Table) Seek(po int64, seekfrom int) (newpos int64, er error) {
	return 0, errors.New("seek error")
}

func (tb *Table) Read(buf []byte) (n int, er error) {
	return 0, errors.New("Read data error")
}

func (tb *Table) ReadBytes(limit byte) (li []byte, er error) {
	return nil, errors.New("read table file bytes error")
}

func (tb *Table) ReadAll() (li []byte, er error) {
	return nil, errors.New("read table file bytes error")
}

func (tb *Table) Write(buf []byte) (n int, er error) {
	return 0, errors.New("write table file error")
}

func (tb *Table) Append(buf []byte) (n int, er error) {
	return 0, errors.New("write table file error")
}

func (tb *Table) Sync() error {
	return errors.New("sync table file error")
}

func (tb *Table) Truncate(i int64) (er error) {
	return errors.New("Truncate table file error")
}

func (tb *Table) NodeName() (name string) {
	return ""
}

func (tb *Table) NodePath() (path string) {
	return ""
}

func (tb *Table) WorldName() (name string) {
	return ""
}

func (tb *Table) ToTextTable() (text string) {
	return ""
}

func (tb *Table) GetId() (id uint64) {
	return 0
}

func (tb *Table) ToInt() (i int) {
	return 0
}

func (tb *Table) ToUint() (i uint) {
	return 0
}

func (tb *Table) ToInt8() (i int8) {
	return 0
}

func (tb *Table) ToUint8() (i uint8) {
	return 0
}

func (tb *Table) ToInt16() (i int16) {
	return 0
}

func (tb *Table) ToUint16() (i uint16) {
	return 0
}

func (tb *Table) ToInt32() (i int32) {
	return 0
}

func (tb *Table) ToUint32() (i uint32) {
	return 0
}

func (tb *Table) ToInt64() (i int64) {
	return 0
}

func (tb *Table) ToUint64() (i uint64) {
	return 0
}

func (tb *Table) ToFloat32() (i float32) {
	return 0
}

func (tb *Table) ToFloat64() (i float64) {
	return 0
}

func (tb *Table) ToBytes() (i []byte) {
	return nil
}

func (tb *Table) ToString() (i string) {
	return ""
}

func (tb *Table) ToComplex64() (i complex64) {
	return i
}

func (tb *Table) ToComplex128() (i complex128) {
	return i
}

func (tb *Table) ToVector() (i Vector) {
	return NewVector()
}

func (tb *Table) ToMatrix() (i Matrix) {
	return NewMatrix()
}

func (tb *Table) ToFile() (i File) {
	return NewFile("")
}

func (tb *Table) ToList() (i List) {
	return NewList()
}

func (tb *Table) ToObject() (i Object) {
	return NewObject()
}

func (tb *Table) ToCode() (i Code) {
	return NewCode()
}

func (tb *Table) ToFunction() (i Function) {
	return NewFunction()
}

func (tb *Table) ToRow() (i Row) {
	return NewRow()
}

func (tb *Table) ToCollum() (i Collum) {
	return NewCollum()
}

func (tb *Table) ToSentene() (i Sentence) {
	return NewSentence()
}

func (tb *Table) FromText(text string) (er Error) {
	switch tb.typeid {
	case CODE:
		// 快速高效解析

	default:
	}
	return nil
}

func (tb *Table) CanExecute() (can bool) {
	return false
}

func (tb *Table) Execute(codetext string, pipe Pipe) (env Enviroment, er Error) {
	code := NewCode()
	code.FromText(codetext)
	env = NewEnviroment()
	for true {
		se := code.CurrentSentence()
		if se == nil {
			break
		}
		env.ExecuteSentence(se, code)
	}
	return env, NewError("remove oid fail")
}

func (tb *Table) ExecuteSentence(se Sentence, code Code) (err Error) {
	return nil
}

func (tb *Table) CurrentSentence() (se Sentence) {
	return nil
}

func (tb *Table) Start() (err Error) {
	return nil
}

func (tb *Table) SimulateExecute() (err Error) {
	return nil
}

func (tb *Table) SetTable(key string, ktb *Table) Error {
	return nil
}

func (tb *Table) GetTable(key string) (ktb *Table) {
	return nil
}

func (tb *Table) RemoveTable(key string) (er Error) {
	return nil
}

func (tb *Table) Clear() (er Error) {
	return nil
}

func (tb *Table) Run() (er Error) {
	return nil
}

func (tb *Table) SetCode(code string) (er Error) {
	tb.code = code
	return nil

}

func (tb *Table) WriteSentence(ktb *Table) Error {
	return nil
}

func (tb *Table) ReadSentence() (ktb *Table) {
	return nil
}

func (tb *Table) SetPipe(condses, doses *Table) Error {
	return NewError("Set fail")
}

func (tb *Table) Close() Error {
	return NewError("close error")
}

# **cattorrent 协议 v1（修订版）**

## **0. 全局约定**

- 传输层：TCP / UDP
- 端口：9822（默认）
- 字节序：**Big Endian（网络字节序）**
- 字符串：**UTF-8 + 长度前缀（uint16）**
- 整数类型：
  - uint16（2B）
  - uint32（4B）
  - uint64（8B）
- hash：统一用 **SHA-256（32 bytes）**

## **1. 通用包格式**

```
uint32 length        # 剩余长度（command + body）
char[4] command      # ASCII 大写命令
bytes   body
```

> length = 4 + len(body)

## **2. UDP 广播发现**

**Command:** **ONLI**


```
uint16 port
uint16 reserved
uint32 protocol_version
uint128 peer_id   # 16字节
```

> 这里加 peer_id 是为了未来做 peer 管理

**行为**

- 每 2 秒广播一次
- 超过 4 秒未收到 → peer 失效

## **3. 获取目录**
### **Request:** 
**Command：** **LIST**

```
empty
```

### **Response:** 
**Command：** **RLST**
**Body：**

```
uint32 file_count

repeat file_count:
    uint16 name_len
    bytes  name
    uint64 file_size
```

## **4. 获取 Meta 信息**
### **Request:** 

**Command：** **META**
**Body：**

```
uint16 name_len
bytes  filename
```

### **Response:** 

- file_hash = **文件内容 SHA-256**
- piece_hash → 后期再加（否则 META 会爆）

**Commnad：RMTA**
**Body：**

```
uint64 file_size
uint32 slice_size         # 固定 256KB 也可以写死，但建议返回
uint32 slice_count
bytes[32] file_hash       # SHA-256
uint32 bitmap_bytes_len
bytes    bitmap           # ceil(piece_count / 8)
uint32 filename_size
```

## **5. 获取 piece**
### **Request:** 
**Command：GETP**
```
uint16 name_len
bytes  filename
uint32 piece_index
```
### **Response:** 
**Command：PIEC**
```
uint32 piece_index
uint32 data_len
bytes  data
```
> data_len ≤ piece_size

## **6. bitmap 说明**
```
bitmap_bytes = ceil(piece_count / 8)
```
每一位表示一个 piece：
```
byte0: piece 0~7
byte1: piece 8~15
...
```

Bitmap bit order:
LSB-first（bit0 → piece 0）

## **7. piece 规则**
```
piece_size = 256 * 1024 bytes
最后一个 piece <= piece_size
```

并且：

```
piece_count = ceil(file_size / piece_size)
```

## **8. 安全约束**
- filename **不能包含** **/** **\** **..**
- 服务端必须验证：
  - canonical path 在共享目录内
	- 只能访问共享目录

## **9. 错误响应**
**Body：**
```
uint16 error_code
uint16 msg_len
bytes  msg
```
示例：

| **code** | **含义**       |
| -- | -- |
| 1        | file not found |
| 2        | bad request    |
| 3        | invalid piece  |
| 4        | internal error |

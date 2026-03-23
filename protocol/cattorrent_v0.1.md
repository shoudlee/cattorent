# cattorrent协议（端口默认用9822、utf-8编码、1个非末尾piece256KB）

### 基本协议格式

1. 1个4字节int，表示后续消息长度，单位为Byte，表示整个包剩余长度（命令+消息）
2. 4个UTF-8字母，表示命令，全大写。
3. 消息体



### 广播发现（udp）

一个客户端上线后，会每隔2s广播一个udp报文，其他客户端可以接收并将其纳入可用peer，不需要响应。检查时，如果超过4s没收到一个客户端的新的发现报文，算作离线

命令：`ONLI`

消息体：1个int，表示发送者使用的端口号



### 获取目录（tcp）

一个客户端向另一个客户端索要共享出的目录（默认用`./catshare/`目录），目前不支持多级目录

#### Request

命令：`LIST`

消息体：留空

#### Response

命令：`LIST`

消息体：UTF-8序列，表示多个文件名和大小，格式如下：

```
{filename1}\t{filesize1}\n{filename2}\t{filesize2}\n
```



### 获取Meta信息

#### Request

命令：`META`

消息体：文件名

#### Response

命令：`META`

消息体：文件大小使用KB为单位，是一个8字节的unsigned long int。文件的Hash使用文件名+文件大小+修改时间。一个piece的hash为内容的hash。Bitmap大小为piece数量，1表示当前位置有，0表示当前位置没有。

格式为：文件大小；\t；文件Hash；\t；Bitmap；\t；每一个piece的Hash；



### 获取piece

一个客户端向另一个客户端索要一份文件的某个piece

#### Request

命令：`GETP`

消息体：UTF-8序列，表示文件名；一个\t；一个4 byte unsigned int，表示获取一个piece，格式如下：

```
{filename}\t{piecenumber}
```

#### 1. Response（正常）

 命令：`PIEC`

消息体：piece index；\t；文件内容

#### 2. Response（文件不存在）

命令：`NOTF`

消息体：留空
package com.proxy.resultparser;

import com.proxy.model.ProxyConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @description:
 * @author: yx
 * @date: 2022/3/26 9:17
 * <p>
 * <p>
 * SocketOutputStream.socketWrite的数据是客户端发送给服务器的
 * 初步估计只取前26位的内容才有需要区分
 * 后面开始的就都是其他内容了
 * 也就是说如果是相同的语句执行多次只有前八位不一样,大概猜测前四位是整个数据包的长度，第5-8位是会话次数的系统偏移量，不用理会
 * <p>
 * 前四位的变化是整体数据包长度，5-12位都是数据偏移量的长度，13-20位目前看到的都是一样的，但是理论上说有存在数据包格式的内容，暂时不处理。
 * 目前需要研究的是数据包的第22-25位是做什么的，也是随着数据包长度变化的，这个要搞清楚,从这里看起,读取了4位:BsonDocumentCodec.decode
 *
 * 第22-25位其实是后面的字节长度,目前推测是前面的总数据包长度-21就等于后面的数据包长度.
 * 去掉前16位后关注这部分内容是变化的内容
 * postion 5-8
 * postion 17-20
 * postion 33-36
 * postion 37-43
 * 经测试少量数据没问题，大量当数据量达到一定限度后会出问题，暂时不考虑那么细了
 */
@Slf4j
public class MongoDbParser extends DefaultParser {
    //处理当返回数据太大分包的情况
    Map<String, ByteBuf> bufferMap = new HashMap();

    public void dealChannel(ChannelHandlerContext ctx, ProxyConfig config, Channel channel, Object msg) {
        Channel ctxChannel = ctx.channel();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctxChannel.remoteAddress();
        String hostString = inetSocketAddress.getHostString();
        int port = inetSocketAddress.getPort();
        ByteBuf readBuffer = (ByteBuf) msg;
        if (Objects.equals(config.getRemoteAddr(), hostString) && Objects.equals(port, config.getRemotePort())) {
            //第一步先获取会话的id,如果当前会话的pid没有被结束则直接把所有的数据写入到缓冲区buffer里面
            String localPid = channel.localAddress().toString();
            if (bufferMap.containsKey(localPid)) {
                ByteBuf byteBuf = bufferMap.get(localPid);
                //如果写入完全了则直接进行sql解析
                byteBuf.writeBytes(readBuffer);
                if (byteBuf.writerIndex() == byteBuf.capacity()) {
                    dealBuffer(ctx, config, channel, byteBuf);
                }
            } else {
                //读取到的数据包总长度
                int buffLen = readBuffer.readableBytes();
                int packLen = readPackLen(readBuffer);
                readBuffer.readerIndex(0);
                if (packLen == buffLen) {
                    //如果数据包长度相等说明数据包接收完全了，可以直接处理接下来的数据了
                    dealBuffer(ctx, config, channel, readBuffer);
                } else if (packLen > buffLen) {
                    //说明数据包接收不完全，需要新建一个buffer存储当前的内容到map,这个少量数据没问题，
                    //当数据量达到一定大小有可能会有问题，研究原理就暂时不考虑这么细了，这部分留给大家自己扩展
                    ByteBuf tmpBuffer = Unpooled.buffer(packLen);
                    tmpBuffer.writeBytes(readBuffer);
                    bufferMap.put(localPid, tmpBuffer);
                }
            }

        } else {
            readBuffer.retain();
            channel.writeAndFlush(readBuffer);
        }

    }

    /**
     * 读取数据一直到下一个字节位0
     *
     * @param readBuffer
     * @return
     */
    public String readStrToNull(ByteBuf readBuffer) {
        //直接创建一个buffer
        ByteBuf tmpBuffer = Unpooled.buffer();
        byte tmpByte;
        while ((tmpByte = readBuffer.readByte()) != 0) {
            tmpBuffer.writeByte(tmpByte);
        }
        return tmpBuffer.toString(Charset.defaultCharset());
    }

    /**
     * 处理数据,代码可能还不稳定，先写简单易懂的流水账等熟悉了再考虑封装优化
     *
     * @param ctx
     * @param config
     * @param channel
     * @param readBuffer
     */
    private void dealBuffer(ChannelHandlerContext ctx, ProxyConfig config, Channel channel, ByteBuf readBuffer) {
        //1.跳过前25位直接从第26位读取;
        readBuffer.readBytes(25);
        //先读取一位判断他的类型
        byte b = readBuffer.readByte();
        //从这里读数据一直读取到结尾数位0然后返回字符串内容
        String result = readStrToNull(readBuffer);
        String localPid = channel.localAddress().toString();
        bufferMap.remove(localPid);
        if (Objects.equals(result, "cursor")) {
            int readableBytes = readBuffer.readableBytes();
            byte[] contentbytes = new byte[readableBytes];
            readBuffer.getBytes(0, contentbytes);
            //说明是查询的对象，需要对数据进行脱敏处理
            //这里直接从49位开始读取，别问为啥，多读一下源码就知道了
            readBuffer.readerIndex(49);
            //从这里读取的数据就是数组中的数据包长度
            int lisPackSize = readPackLen(readBuffer);
            //如果数组大于5说明这里面有数据，需要读取他的内容，否则不做处理直接返回数据
            if (lisPackSize > 5) {
                while (true) {
                    if (readBuffer.readByte() == 0) {
                        //如果为0表示这个对象结束了
                        break;
                    }
                    //去掉数据包的前23位
                    readBuffer.readBytes(23);
                    //这里预览看是否会报错再做处理
                    while (true) {
                        byte tag = readBuffer.readByte();
                        if (tag == 0 || readBuffer.readerIndex() >= lisPackSize) {
                            //如果为0表示这个对象结束了
                            break;
                        }
                        //2表示字符串，这里暂时只处理字符串类型的，其他的再做考虑
                        if (tag == 2) {
                            //从这里读取数据一直读取到为0的位置就是key
                            String key = readStrToNull(readBuffer);
                            //读取下一个数据包长度为
                            dealValue(readBuffer);
                        } else {
                        }
                    }

                }
            }
            readBuffer.readerIndex(0);
            channel.writeAndFlush(readBuffer);
        } else {
            //其他数据，直接返回即可
            readBuffer.readerIndex(0);
            channel.writeAndFlush(readBuffer);
        }

    }

    /**
     * 读取这个key的value并且进行处理替换成新的字符串写进去
     * @param readBuffer
     */
    void dealValue(ByteBuf readBuffer) {
        int valueSize = readPackLen(readBuffer);
        int index = readBuffer.readerIndex();
        ByteBuf byteBuf = readBuffer.readBytes(valueSize);
        int readableBytes = byteBuf.readableBytes() - 1;
        byte[] datas = new byte[readableBytes];
        byteBuf.getBytes(0, datas);
        String value = new String(datas);
        String maskValue = replaceStr(value);
        byte[] bytes = maskValue.getBytes();
        for (int i = 0; i < readableBytes; i++) {
            if (i < bytes.length) {
                datas[i] = bytes[i];
            } else {
                datas[i] = 32;
            }
        }
        int writerIndex = readBuffer.writerIndex();
        int readerIndex = readBuffer.readerIndex();
        //最终要把这个key写进去
        readBuffer.readerIndex(index);
        readBuffer.writerIndex(index);
        readBuffer.writeBytes(datas);
        //再把写入索引还原
        readBuffer.writerIndex(writerIndex);
        readBuffer.readerIndex(readerIndex);
    }

    /**
     * 对读取到的字符串进行遮罩处理
     * @param content
     * @return
     */
    String replaceStr(String content) {
        int start = (int) (content.length() * 0.3);
        int end = (int) (content.length() * 0.7);
        //不确定需要脱敏的长度是多少，先这样处理后续再优化
        List<String> replaceContent = new ArrayList<>();
        for (int a = start; a < end; a++) {
            replaceContent.add("*");
        }
        String join = StringUtils.join(replaceContent, "");
        return content.substring(0, start) + join + content.substring(end);
    }

    /**
     * 根据数组获取数据包的长度
     * @param byteBuf
     * @return
     */
    int readPackLen(ByteBuf byteBuf) {
        byte b1 = byteBuf.readByte();
        byte b2 = byteBuf.readByte();
        byte b3 = byteBuf.readByte();
        byte b4 = byteBuf.readByte();
        return makeInt(b4, b3, b2, b1);
    }

    /**
     * 根据4位字节获取数据包的长度
     * @param b3
     * @param b2
     * @param b1
     * @param b0
     * @return
     */
    int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) << 8) |
                ((b0 & 0xff)));
    }
}


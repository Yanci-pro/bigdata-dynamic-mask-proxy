package com.proxy.resultparser;

import com.proxy.model.ProxyConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @description: 处理redsi返回的数据报文对某些key进行脱敏处理，这里只处理key包含phone的
 * @author: yx
 * @date: 2022/2/18 9:17
 */
@Slf4j
public class RedisParser extends DefaultParser {
    //处理当返回数据太大分包的情况，暂时不考虑
    Map<String, ByteBuf> bufferMap = new HashMap();
    ///因为存储需要挎会话常规变量无法共享暂时做成静态的变量便于测试，后续再优化其他方案
    static Map<String, String> cmdMap = new HashMap();
    static Set cmdSet = new HashSet();
    //需要脱敏的key,后续需要做成可配置的，这里暂时写死便于测试
    static Set keySet = new HashSet();

    static {
        cmdSet.add("GET");
        cmdSet.add("LRANGE");
        cmdSet.add("SMEMBERS");
        cmdSet.add("HGET");
        cmdSet.add("ZRANGE");
        cmdSet.add("SSCAN");
        cmdSet.add("HGETALL");
        cmdSet.add("HSCAN");
        keySet.add("phone");
    }

    String split = new String(new byte[]{13, 10});

    public void dealChannel(ChannelHandlerContext ctx, ProxyConfig config, Channel channel, Object msg) {
        Channel ctxChannel = ctx.channel();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctxChannel.remoteAddress();
        String hostString = inetSocketAddress.getHostString();
        int port = inetSocketAddress.getPort();
        ByteBuf readBuffer = (ByteBuf) msg;
        if (Objects.equals(config.getRemoteAddr(), hostString) && Objects.equals(port, config.getRemotePort())) {
            dealResultBuffer(ctx, config, channel, readBuffer);
        } else {
            dealCmdBuffer(ctx, config, channel, readBuffer);
        }
    }

    void dealCmdBuffer(ChannelHandlerContext ctx, ProxyConfig config, Channel channel, ByteBuf readBuffer) {
        String localPid = ctx.channel().remoteAddress().toString();
        String cmdContent = readBuffer.toString(Charset.defaultCharset());
        String[] split = cmdContent.split(this.split);
        //这里有可能有其他命令不足三位的，暂时不处理
        if (split.length > 2) {
            //获取命令的数量
            Integer integer = Integer.valueOf(split[0].replace("*", ""));
            //没啥用，只是暂时这样处理验证报文正确性后续好处理逻辑
            if (integer * 2 + 1 != split.length) {
//            throw new RuntimeException("命令格式解析错误");
            }
            //获取命令类型
            String cmd = split[2];
            //如果扫描到命令是需要拦截的就存入map后续处理
            if (cmdSet.contains(cmd.toUpperCase())) {
                //获取key,一般是第五个为key，有问题后续再处理，hashkey单独处理，目前只处理前面的大key
                String key = split[4];
                //因为只配置了一个key,所以改成包含phone的就脱敏，便于测试
                if (key.contains("phone")) {
                    cmdMap.put(localPid, key);
                }
            }
        }
        readBuffer.retain();
        channel.writeAndFlush(readBuffer);

    }

    void dealResultBuffer(ChannelHandlerContext ctx, ProxyConfig config, Channel channel, ByteBuf readBuffer) {
        String localPid = channel.remoteAddress().toString();

        String content = readBuffer.toString(Charset.defaultCharset());
        if (cmdMap.containsKey(localPid)) {
            String resultContent = maskValue(content);
            readBuffer.writerIndex(0);
            readBuffer.readerIndex(0);
            readBuffer.writeBytes(resultContent.getBytes());
            readBuffer.writeByte(13);
            readBuffer.writeByte(10);
            cmdMap.remove(localPid);
            channel.writeAndFlush(readBuffer);
        } else {
            readBuffer.retain();
            channel.writeAndFlush(readBuffer);
        }

    }

    //处理整个结果返回脱敏后的结果,需要忽略一些系统报文格式的内容
    String maskValue(String content) {
        List<String> strings = Arrays.asList(content.split(this.split));
        for (int i = 1; i < strings.size(); i++) {
            String contetnValue = strings.get(i);
            if (contetnValue.startsWith("$") || contetnValue.startsWith("*")) {
                continue;
            }
            strings.set(i, replaceStr(contetnValue));
            //间隔是2，所以这里这里索引手动加1
            i++;
        }
        return StringUtils.join(strings, split);
    }

    /**
     * 这里固定脱敏百分之30到70%，后续再改写
     *
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


}

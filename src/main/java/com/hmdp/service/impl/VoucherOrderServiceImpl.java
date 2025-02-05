package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Event;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.event.KafkaOrderProducer;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.KafkaConstants.TOPIC_CREATE_ORDER;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private KafkaOrderProducer kafkaOrderProducer;

    IVoucherOrderService proxy;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //异步处理线程池
//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();


    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
//    @PostConstruct
//    private void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
//    }

    // 用于线程池处理的任务
    // 当初始化完毕后，就会去从消息队列对列中去拿消息
    /*private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {

            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2. 判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 2.1.如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 2.2.如果获取成功，可以下单
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                    // 3.ACK确认 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 处理异常消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 2. 判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 2.1.如果获取失败，说明pending-list中没有消息,结束循环
                        break;
                    }
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 2.2.如果获取成功，可以下单
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                    // 3.ACK确认 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 处理pending-list异常消息
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }
    }*/
    //阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    // 用于线程池处理的任务
    // 当初始化完毕后，就会去从对列中去拿信息
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//
//            while (true) {
//                try {
//                    // 1.获取队列中的订单信息,使用take()，当阻塞队列中没有元素时,线程会阻塞
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//
//        public void handleVoucherOrder(VoucherOrder voucherOrder) {
//            Long userId = voucherOrder.getUserId();
//            // 创建分布式锁对象
//            RLock lock = redissonClient.getLock("lock:order:" + userId);
//            // 获取锁
//            boolean isLock = lock.tryLock();
//            //获取锁失败
//            if (!isLock) {
//                log.error("不允许重复下单！");
//                return;
//            }
//            // 获取锁成功
//            try {
//                // 通过代理对象调用该方法事务才会生效，不能用this调用
//                proxy.createVoucherOrder(voucherOrder);
//            } finally {
//                //释放锁
//                lock.unlock();
//            }
//        }
//    }

    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 创建分布式锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        //获取锁失败
        if (!isLock) {
            log.error("不允许重复下单！");
            return;
        }
        // 获取锁成功
        try {
            // 通过代理对象调用该方法事务才会生效，不能用this调用
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 4.2.实现一人一单
        Long userId = voucherOrder.getUserId();
        Integer count = query().eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过了");
            return;
        }
        // 5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0) //乐观锁版本号法避免超卖,stock = voucher.getVoucher()会导致有些卖不出去，所以只需stock > 0 即可
                .update();
        if (!success) {
            // 扣减失败
            log.error("库存不足");
            return;
        }
        // 6.创建订单
        save(voucherOrder);
    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//        // 订单id
//        long orderId = redisIdWorker.nextId("order");
//        // 1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
////        Long result = stringRedisTemplate.execute(
////                SECKILL_SCRIPT,
////                Collections.emptyList(),
////                voucherId.toString(), userId.toString(), String.valueOf(orderId)
////        );
//        int r = result.intValue();
//        // 2.判断结果是否为0
//        if (r != 0) {
//            // 2.1.不为0 ，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//        // 3.获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 4.返回订单id
//        return Result.ok(orderId);
//    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2. 为0，有购买资格，把订单信息保存到阻塞队列
        // 3.1.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 3.2.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 3.3.用户id
        voucherOrder.setUserId(userId);
        // 3.4.优惠券id
        voucherOrder.setVoucherId(voucherId);
        // 3.5.放入阻塞队列
//        orderTasks.add(voucherOrder);
        // 3.5.秒杀成功，发送消息到kafka
        sendOrderMsgToKafka(orderId, voucherId, userId);
        // 3.6.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3.返回订单id
        return Result.ok(orderId);
    }

    public void sendOrderMsgToKafka(long orderId, Long voucherId, Long userId) {
        Map<String, Object> data = new HashMap<>();
        data.put("voucherId", voucherId);
//        data.put("buyNumber", buyNumber);
        Event event = new Event()
                .setTopic(TOPIC_CREATE_ORDER)
                .setUserId(userId)
                .setEntityId(orderId)
                .setData(data);
        kafkaOrderProducer.publishEvent(event);
    }
//
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀尚未开始");
//        }
//        // 3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀已结束");
//        }
//        // 4.1.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足!");
//        }
//        Long userId = UserHolder.getUser().getId();
//        // 创建分布式锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        // 获取锁
    //boolean isLock = lock.tryLock(1200);
//        boolean isLock = lock.tryLock();
//        //获取锁失败
//        if(!isLock){
//            return Result.fail("不允许重复下单！");
//        }
//        // 获取锁成功
//        try {
//            // 通过代理对象调用该方法事务才会生效，不能用this调用
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//          lock.unlock();
//        }
//    }


//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        // 4.2.实现一人一单
//        Long userId = UserHolder.getUser().getId();
//        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        if (count > 0) {
//            //该用户已经购买过了
//            return Result.fail("用户已经购买过一次！");
//        }
//        // 5.扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId).gt("stock", 0) //乐观锁版本号法避免超卖,stock = voucher.getVoucher()会导致有些卖不出去，所以只需stock > 0 即可
//                .update();
//        if (!success) {
//            return Result.fail("库存不足!");
//        }
//        // 6.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 6.1.订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 6.2.用户id
//        voucherOrder.setUserId(userId);
//        // 6.3.优惠券id
//        voucherOrder.setVoucherId(voucherId);
//        // 6.4.写入订单
//        save(voucherOrder);
//        // 7.返回订单id
//        return Result.ok(orderId);
//    }
}

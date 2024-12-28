package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.RedisData;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
//         Shop shop = queryPassThrough(id);
        Shop shop = cacheClient.queryPassThrough(
                RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById,
                RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);

//        // 互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
//        Shop shop = cacheClient.queryWithMutex(
//                RedisConstants.CACHE_SHOP_KEY,RedisConstants.LOCK_SHOP_KEY, id, Shop.class, this::getById,
//                RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 利用逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
//        Shop shop = cacheClient.queryWithLogicalExpire(
//                RedisConstants.CACHE_SHOP_KEY, RedisConstants.LOCK_SHOP_KEY, id, Shop.class, this::getById,
//                RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    // 逻辑过期解决缓存击穿
    private Shop queryWithLogicalExpire(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        // 1. 从缓存中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 未命中，直接返回
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        // 3.命中，先把jason反序列化未java对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            // 5.未过期，直接返回店铺信息
            return shop;
        }

        // 6.已过期，尝试缓存重建
        // 6.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        // 6.2获取成功，开启独立线程，实现缓存重建
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    this.unlock(lockKey);
                }
            });
        }
        // 6.3返回过期的店铺信息

        return shop;
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        Thread.sleep(200);
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id, JSONUtil.toJsonStr(redisData));
    }

    // 互斥锁缓存击穿
    private Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从缓存中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 命中，直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断命中的是否为空值
        if (shopJson != null){
            // 返回一个错误信息
            return null;
        }

        // 3.未命中，实现缓存重建
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            // 3.1.获取互斥锁
            boolean isLock = tryLock(lockKey);
            // 3.2.判断是否获取成功
            if (!isLock){
                // 3.3.失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 3.4.成功，根据id查询数据
            shop = getById(id);
            // 模拟重建延时
            Thread.sleep(200);
            // 4. 不存在
            if (shop == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误
                return null;
            }
            // 5. 存在，写入redis并设置过期时间
            shopJson = JSONUtil.toJsonStr(shop);
            stringRedisTemplate.opsForValue().set(key,shopJson,RedisConstants.CACHE_SHOP_TTL + RandomUtil.randomLong(1,5), TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 6.释放锁
            unlock(lockKey);
        }
        // 7.返回
        return shop;
    }

    // 缓存空值解决缓存穿透
    private Shop queryPassThrough(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从缓存中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 命中，直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断命中的是否为空值
        if (shopJson != null){
            // 返回一个错误信息
            return null;
        }
        // 3. 未命中，根据id从数据库中查询
        Shop shop = getById(id);
        // 4. 不存在
        if (shop == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误
            return null;
        }
        // 5. 存在，写入redis并设置过期时间
        shopJson = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(key,shopJson,RedisConstants.CACHE_SHOP_TTL + RandomUtil.randomLong(1,5), TimeUnit.MINUTES);
        // 6.返回
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        // 更新数据库
        updateById(shop);
        // 删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+id);

        return Result.ok();
    }
}

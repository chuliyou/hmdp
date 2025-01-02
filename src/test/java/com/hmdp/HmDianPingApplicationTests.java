package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import sun.net.www.http.HttpClient;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.http.impl.client.HttpClients;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testSaveShop() throws InterruptedException {
        shopService.saveShop2Redis(1L, 10L);
    }
    @Test
    void test1(){
        System.out.println(Integer.MAX_VALUE + 1);
        Integer i = Integer.valueOf(1);
        int i1 = i.intValue();
    }

    @Test
    void testHyperLogLog() {
        // 准备数组，装用户数据
        String[] users = new String[1000];
        // 数组角标
        int index = 0;
        for (int i = 1; i < 1000000; i++) {
            //赋值
            users[index++] = "user_" + i;
            // 每1000条发送一次
            if (i % 1000 == 0) {
                index = 0;
                stringRedisTemplate.opsForHyperLogLog().add("hll1",users);
            }
        }
        // 统计数量
        Long size = stringRedisTemplate.opsForHyperLogLog().size("hll1");
        System.out.println("size :" + size);
    }

    @Test
    void loadShopData() {
        // 1.查询店铺信息
        List<Shop> list = shopService.list();
        // 2.把店铺分组，按照typeId分组，typeId一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3.分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1.获取类型id
            Long typeId = entry.getKey();
            String key = RedisConstants.SHOP_GEO_KEY + typeId;
            // 3.2.获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            // 3.3.写入redis GEOADD key 经度 纬度 member
            for (Shop shop : value) {
                // stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }


    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id：" + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    @Autowired
    private IUserService userService;

    @Test
    public void function() {
        String loginUrl = "http://localhost:8080/api/user/login"; // 替换为实际的登录URL
        String tokenFilePath = "tokens.txt"; // 存储Token的文件路径

        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            BufferedWriter writer = new BufferedWriter(new FileWriter(tokenFilePath));
            // 从数据库中获取用户手机号
            List<User> users = userService.list();
            for (User user : users) {
                String phoneNumber = user.getPhone();
                // 构建登录请求
                HttpPost httpPost = new HttpPost(loginUrl);
                //（1.如果作为请求参数传递）
                //List<NameValuePair> params = new ArrayList<>();
                //params.add(new BasicNameValuePair("phone", phoneNumber));
                // 如果登录需要提供密码，也可以添加密码参数
                // params.add(new BasicNameValuePair("password", "user_password"));
                //httpPost.setEntity(new UrlEncodedFormEntity(params));
                // (2.如果作为请求体传递)构建请求体JSON对象
                JSONObject jsonRequest = new JSONObject();
                jsonRequest.put("phone", phoneNumber);
                StringEntity requestEntity = new StringEntity(
                        jsonRequest.toString(),
                        ContentType.APPLICATION_JSON);
                httpPost.setEntity(requestEntity);
                // 发送登录请求
                HttpResponse response = httpClient.execute(httpPost);
                // 处理登录响应，获取token
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    System.out.println(responseString);
                    // 解析响应，获取token，这里假设响应是JSON格式的
                    // 根据实际情况使用合适的JSON库进行解析
                    String token = parseTokenFromJson(responseString);
                    System.out.println("手机号 " + phoneNumber + " 登录成功，Token: " + token);
                    // 将token写入txt文件
                    writer.write(token);
                    writer.newLine();
                } else {
                    System.out.println("手机号 " + phoneNumber + " 登录失败");
                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 解析JSON响应获取token的方法，这里只是示例，具体实现需要根据实际响应格式进行解析
    private static String parseTokenFromJson(String json) {
        try {
            // 将JSON字符串转换为JSONObject
            JSONObject jsonObject = new JSONObject(json);
            // 从JSONObject中获取名为"token"的字段的值
            String token = jsonObject.getString("data");
            return token;
        } catch (Exception e) {
            e.printStackTrace();
            return null; // 解析失败，返回null或者抛出异常，具体根据实际需求处理
        }
    }

}

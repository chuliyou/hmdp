package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        //查询缓存中是否存在
        String key = "shop:typeList";
        List<String> typeListJson = stringRedisTemplate.opsForList().range(key,0,-1);
        //命中，直接返回
        if (typeListJson != null && typeListJson.size() > 0) {
            List<ShopType> typeList = typeListJson.stream()
                    .map(typeListJsonItem -> JSONUtil.toBean(typeListJsonItem, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(typeList);
        }
        //未命中，从数据库中查询
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //不存在，返回错误信息
        if(typeList == null || typeList.size()==0){
            return Result.fail("暂无商户类型！");
        }
        //存在，存入缓存中
        typeListJson = typeList.stream().map(JSONUtil::toJsonStr).collect(Collectors.toList());
        stringRedisTemplate.opsForList().leftPushAll(key,typeListJson);
        //返回
        return Result.ok(typeList);
    }
}

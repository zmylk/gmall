package com.atguigu.gmall.mock;

import com.atguigu.gmall.mock.utils.RandomNum;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class TextMocker {


    public static void main(String[] args) {
        String str="hello world!";
        FileWriter writer;
        try {
            writer = new FileWriter("d:/token.txt");
            writer.write("");//清空原文件内容
            for (int i = 0; i < 200; i++) {
                writer.write(getOneLine());
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    public static String getOneLine()
    {
        String line = "";
        //user_id
        line += RandomNum.getRandInt(0, 500) + "&";
        //sku_id: String,
        line += RandomNum.getRandInt(0, 500) + "&";
        //user_gender: String,
        ArrayList<String> objects = new ArrayList<>();
        objects.add("F");
        objects.add("M");
        line += objects.get(RandomNum.getRandInt(0, 1)) + "&";
        // user_age: Int,
        line += RandomNum.getRandInt(0, 100) + "&";
        //user_level: String,
        line += RandomNum.getRandInt(0, 5) + "&";
        //sku_price: Double,
        line += RandomNum.getRandDouble(0, 10000) + "&";
        //sku_name: String,
        ArrayList<String> name = new ArrayList<>();
        name.add("荣耀20S 李现同款 3200万人像超级夜景 4800万超广角AI三摄 麒麟810 全网通版6GB+128GB 蝶羽蓝");
        name.add("小米CC9 Pro 1亿像素 五摄四闪 10倍混合光学变焦 5260mAh 屏下指纹 魔法绿镜 8GB+128GB 游戏智能拍照手机");
        name.add("华为 HUAWEI Mate 30 5G 麒麟990 4000万超感光徕卡影像双超级快充8GB+256GB亮黑色5G全网通游戏手机");
        name.add("华为 HUAWEI nova 5 Pro 前置3200万人像超级夜景4800万AI四摄麒麟980芯片8GB+128GB绮境森林全网通双4G手机");
        name.add("realme 真我X50 6400万变焦四摄 双模5G 高通骁龙765G 120Hz电竞屏 前置双摄 全网通8GB+128GB 冰川 游戏智能手机");
        name.add("黑鲨游戏手机2 Pro 8GB+128GB 电鸣黑 骁龙855Plus 屏幕压感 极速触控 全面屏 双卡双待 4G全网通");
        name.add("诺基亚 NOKIA 2720 移动联通电信三网4G 红色 双卡双待 经典复刻 翻盖手机 4G热点备用功能机 老人机");
        line += name.get(RandomNum.getRandInt(0, 6)) + "&";
        //sku_tm_id: String,
        line += RandomNum.getRandInt(0, 500) + "&";
        //sku_category1_id: String,
        line += RandomNum.getRandInt(0, 500) + "&";
        //sku_category2_id: String,
        line += RandomNum.getRandInt(0, 500) + "&";
        //sku_category3_id: String,
        line += RandomNum.getRandInt(0, 500) + "&";
        // sku_category1_name: String,
        ArrayList<String> category = new ArrayList<>();
        category.add("水果");
        category.add("手机");
        category.add("医疗");
        category.add("幼儿");
        category.add("赌博");
        category.add("生活");
        category.add("李克");
        line += category.get(RandomNum.getRandInt(0, 6)) + "&";
        // sku_category2_name: String,
        line += category.get(RandomNum.getRandInt(0, 6)) + "&";
        // sku_category3_name: String,
        line += category.get(RandomNum.getRandInt(0, 6)) + "&";

        //                spu_id: String,
        line += RandomNum.getRandInt(0, 500) + "&";
        //                sku_num: Long,
        line += RandomNum.getRandInt(0, 500) + "&";
        //                order_count: Long,
        line += RandomNum.getRandInt(0, 500) + "&";
        //                order_amount: Double,
        line += RandomNum.getRandDouble(0, 10000) + "&";
        //                var dt:String
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String format = simpleDateFormat.format(date);

        line += format +"\n";
        System.out.println(line);
        return line;
    }
}

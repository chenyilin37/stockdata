package vegoo.stockdata.db.tryit;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

public class TestSet {
	public static void main(String[] args) {

		Set<String> set1 = new HashSet<String>() {
			{
				add("王者荣耀");
				add("英雄联盟");
				add("穿越火线");
				add("地下城与勇士");
			}
		};

		Set<String> set2 = new HashSet<String>() {
			{
				add("王者荣耀");
				add("地下城与勇士");
				add("魔兽世界");
			}
		};

		Set<String> result1 = Sets.union(set1, set2);// 合集，并集
		Set<String> result2 = Sets.intersection(set1, set2);// 交集
		Set<String> result3 = Sets.difference(set1, set2);// 差集 1中有而2中没有的
		Set<String> result4 = Sets.difference(set2, set1);// 差集 1中有而2中没有的
		Set<String> result5 = Sets.symmetricDifference(set1, set2);// 相对差集 1中有2中没有 2中有1中没有的 取出来做结果

		System.out.println("并集：" + result1);
		System.out.println("交集：" + result2);

		System.out.println("set1差集：" + result3);

		System.out.println("set2差集：" + result4);

		System.out.println("相对差集：" + result5);
	}

}

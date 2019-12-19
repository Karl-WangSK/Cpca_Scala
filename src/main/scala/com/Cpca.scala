package com

import java.io.InputStream
import java.util
import java.util.Scanner

import com.huaban.analysis.jieba.JiebaSegmenter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class Cpca {
  var map: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  map.put("省", "")
  map.put("市", "")
  map.put("区", "")

  def setKV(k: String, v: String): Unit = {
    map.put(k, v)
  }

  def getV(k: String) = {
    map(k)
  }
}

object Cpca {
  //    # 直辖市
  val munis = List("北京市", "天津市", "上海市", "重庆市")
  //自定义的区级到市级的映射,主要用于解决区重名问题,如果定义的映射在模块中已经存在，则会覆盖模块中自带的映射
  val myumap: Map[String, String] = Map(
    "南关区" -> "长春市",
    "南山区" -> "深圳市",
    "宝山区" -> "上海市",
    "市辖区" -> "东莞市",
    "普陀区" -> "上海市",
    "朝阳区" -> "北京市",
    "河东区" -> "天津市",
    "白云区" -> "广州市",
    "西湖区" -> "杭州市",
    "铁西区" -> "沈阳市"
  )
  var area_map: util.HashMap[String, (String, String, String)] = new util.HashMap[String, (String, String, String)]
  var city_map: util.HashMap[String, (String, String, String)] = new util.HashMap[String, (String, String, String)]
  var province_area_map: util.HashMap[(String, String), (String, String, String)] = new util.HashMap[(String, String), (String, String, String)]
  var latlng: util.HashMap[String, String] = new util.HashMap[String, String]()
  var province_map: util.HashMap[String, String] = new util.HashMap[String, String]

  def main(args: Array[String]): Unit = {
    val strings = Array("浙江温州瓯海区", "上海市浦东新区")
    val result: mutable.Seq[Cpca] = transform(strings)
    for (elem <- result) {
      println(elem.getV("省") + "===" + elem.getV("市") + "====" + elem.getV("区"))
    }
  }

  def is_munis(city_full_name: String) = {
    if (munis.contains(city_full_name)) {
      city_full_name
    }
  }

  def _data_from_csv(str: String) = {
    val strs: Array[String] = str.split(",")
    latlng.put(strs(1) + "," + strs(2) + "," + strs(3), strs(4) + "," + strs(5))
    _fill_province_map(province_map, strs)
    _fill_area_map(area_map, strs)
    _fill_city_map(city_map, strs)
    _fill_province_area_map(province_area_map, strs)
    (area_map, city_map, province_area_map, province_map, latlng)

  }

  def _fill_province_map(hashMap: util.HashMap[String, String], strs: Array[String]) = {
    val sheng: String = strs(1)
    if (!hashMap.containsKey(sheng)) {
      hashMap.put(sheng, sheng)
      //      # 处理省的简写情况
      //      # 普通省分 和 直辖市
      if (sheng.endsWith("省") || sheng.endsWith("市")) hashMap.put(sheng.substring(0, sheng.length - 1), sheng)
      //      # 自治区
      else if (sheng == "新疆维吾尔自治区") hashMap.put("新疆", sheng)
      else if (sheng == "内蒙古自治区") hashMap.put("内蒙古", sheng)
      else if (sheng == "西藏自治区") hashMap.put("西藏", sheng)
      else if (sheng == "广西壮族自治区") {
        hashMap.put("广西", sheng)
        hashMap.put("广西省", sheng)
      }
      else if (sheng == "宁夏回族自治区") hashMap.put("宁夏", sheng)
      //      # 特别行政区
      else if (sheng == "香港特别行政区") hashMap.put("香港", sheng)
      else if (sheng == "澳门特别行政区") hashMap.put("澳门", sheng)

    }
  }

  def _fill_area_map(hashMap: util.HashMap[String, (String, String, String)], strs: Array[String]) = {
    val area_name = strs(3)
    val pca_tuple = (strs(1), strs(2), strs(3))
    hashMap.put(area_name, pca_tuple)
    if (area_name.endsWith("市")) hashMap.put(area_name.substring(0, area_name.length - 1), pca_tuple)
  }

  def _fill_city_map(hashMap: util.HashMap[String, (String, String, String)], strs: Array[String]) = {
    val city_name = strs(2)
    val pca_tuple = (strs(1), strs(2), strs(3))
    hashMap.put(city_name, pca_tuple)
    if (city_name.endsWith("市"))
      hashMap.put(city_name.substring(0, city_name.length - 1), pca_tuple)
    //    # 特别行政区
    else if (city_name == "香港特别行政区")
      hashMap.put("香港", pca_tuple)
    else if (city_name == "澳门特别行政区")
      hashMap.put("澳门", pca_tuple)
  }

  def _fill_province_area_map(hashMap: util.HashMap[(String, String), (String, String, String)], strs: Array[String]) = {
    val pca_tuple = (strs(1), strs(2), strs(3))
    val key = (strs(1), strs(3))

    hashMap.put(key, pca_tuple)
  }

  """将地址描述字符串转换以"省","市","区"
        Args:
            locations:地址描述字符集合,可以是list, Series等任意可以进行for in循环的集合
                      比如:["徐汇区虹漕路461号58号楼5楼", "泉州市洛江区万安塘西工业区"]
            umap:自定义的区级到市级的映射,主要用于解决区重名问题,如果定义的映射在模块中已经存在，则会覆盖模块中自带的映射
            cut:是否使用分词，默认使用，分词模式速度较快，但是准确率可能会有所下降
            lookahead:只有在cut为false的时候有效，表示最多允许向前看的字符的数量
                      默认值为8是为了能够发现"新疆维吾尔族自治区"这样的长地名
                      如果你的样本中都是短地名的话，可以考虑把这个数字调小一点以提高性能
        Returns:
            省市区信息，如下：
               省    /市   /区
               上海市/上海市/徐汇区
               福建省/泉州市/洛江区
    """

  def transform(location_strs: String, umap: Map[String, String] = myumap, cut: Boolean = true, lookahead: Int = 8): String = {
    //加载词库
    val bufferarr = new ArrayBuffer[String]()
    val path: InputStream = Cpca.getClass.getClassLoader.getResourceAsStream("pca.csv")
    val scanner = new Scanner(path)
    //将省市区做映射
    while (scanner.hasNext()) {
      val str: String = scanner.nextLine()
      _data_from_csv(str)
    }
    val cpca: Cpca = _handle_one_record(location_strs, umap, cut, lookahead)
    cpca.getV("省") + "/" + cpca.getV("市") + "/" + cpca.getV("区")
  }

  def transform(location_arr: Array[String], umap: Map[String, String] = myumap, cut: Boolean = true, lookahead: Int = 8): mutable.Seq[Cpca] = {
    //加载词库
    val bufferarr = new ArrayBuffer[String]()
    val path: InputStream = Cpca.getClass.getClassLoader.getResourceAsStream("pca.csv")
    val scanner = new Scanner(path)
    //将省市区做映射
    while (scanner.hasNext()) {
      val str: String = scanner.nextLine()
      _data_from_csv(str)
    }
    val arr = new ArrayBuffer[Cpca]()
    for (elem <- location_arr) {
      val cpca: Cpca = _handle_one_record(elem, umap, cut, lookahead)
      arr += cpca
    }
    arr
  }


  def _handle_one_record(addr: String, umap: Map[String, String], cut: Boolean, lookahead: Int) = {
    """处理一条记录"""
    //      # 空记录
    if (addr == "" || addr == null) {
      val cpca = new Cpca()
      cpca
    } else {
      //      # 地名提取
      val cpca: Cpca = _extract_addr(addr, cut, lookahead)
      _fill_city(cpca, umap)
      _fill_province(cpca)
      cpca
    }

  }

  def _extract_addr(addr: String, cut: Boolean, lookhead: Int): Cpca = {
    if (cut) _jieba_extract(addr)
    else _full_text_extract(addr, lookhead)
  }

  def _jieba_extract(addr: String): Cpca = {
    val cpca = new Cpca()
    import scala.collection.JavaConverters._
    val jieba = new JiebaSegmenter
    val strs: mutable.Seq[String] = jieba.sentenceProcess(addr).asScala
    for (elem <- strs) {
      if (area_map.containsKey(elem)) cpca.map.put("区", area_map.get(elem)._3)
      else if (city_map.containsKey(elem)) cpca.map.put("市", city_map.get(elem)._2)
      else if (province_map.containsKey(elem)) cpca.map.put("省", province_map.get(elem))
    }
    cpca
  }

  def _fill_province(result: Cpca) = {
    """填充省"""
    if (result.getV("市") != "" && result.getV("省") == "" && city_map.containsKey(result.getV("市"))) {
      result.setKV("省", city_map.get(result.getV("市"))._1)
    }
    if (result.getV("市") == "" && result.getV("省") == "" && area_map.containsKey(result.getV("区"))) {
      result.setKV("省", area_map.get(result.getV("区"))._1)
    }
  }

  def _fill_city(cpca: Cpca, umap: Map[String, String]): Unit = {
    """填充市"""
    if (cpca.getV("市") == "") {
      //      # 从 区 映射
      if (cpca.getV("区") != "") {
        if (area_map.containsKey(cpca.getV("区"))) cpca.setKV("市", area_map.get(cpca.getV("区"))._2)
        if (umap.contains(cpca.getV("区"))) cpca.setKV("市", umap(cpca.getV("区")))
      }
      //      # 从 省,区 映射
      if (cpca.getV("区") != "" && cpca.getV("省") != "") {
        if (province_area_map.containsKey((cpca.getV("省"), cpca.getV("区")))) cpca.setKV("市", province_area_map.get((cpca.getV("省"), cpca.getV("区")))._2)
      }
    }
  }

  def _full_text_extract(addr: String, lookahead: Int) = {
    val cpca = new Cpca
    //      # i为起始位置
    var i = 0

    while (i < addr.length) {
      var defer_fun = 0
      //      # l为从起始位置开始的长度,从中提取出最长的地址
      breakable {
        for (x <- 1 until lookahead + 1) {
          if (i + x > addr.length) break()
          var elem = addr.substring(i, i + x)
          breakable {
            if (area_map.containsKey(elem)) {
              cpca.map.put("区", area_map.get(elem)._3)
              defer_fun = elem.length
              break()
            }
            else if (city_map.containsKey(elem)) {
              cpca.map.put("市", city_map.get(elem)._2)
              defer_fun = elem.length
              break()
            }
            else if (province_map.containsKey(elem)) {
              cpca.map.put("省", province_map.get(elem))
              defer_fun = elem.length
              break()
            }
          }
        }
      }
      if (defer_fun != 0) {
        i += defer_fun
      } else {
        i += 1
      }
    }
    cpca
  }
}

-- MySQL dump 10.13  Distrib 5.5.59, for debian-linux-gnu (armv7l)
--
-- Host: localhost    Database: stockdata
-- ------------------------------------------------------
-- Server version	5.5.59-0+deb8u1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `block`
--

DROP TABLE IF EXISTS `block`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `block` (
  `blkucode` varchar(10) NOT NULL,
  `marketid` varchar(1) DEFAULT NULL,
  `blkcode` varchar(8) NOT NULL,
  `blkname` varchar(45) NOT NULL,
  `blktype` varchar(8) DEFAULT NULL COMMENT '1-行业\n2-地域\n3-概念',
  PRIMARY KEY (`blkucode`),
  UNIQUE KEY `index2` (`blkcode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `gdhs`
--

DROP TABLE IF EXISTS `gdhs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `gdhs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stockCode` varchar(6) DEFAULT NULL,
  `EndDate` date DEFAULT NULL COMMENT '统计截止日期',
  `EndTradeDate` date DEFAULT NULL COMMENT '统计截止日期对应的交易日',
  `HolderNum` decimal(13,0) DEFAULT NULL COMMENT '股东数',
  `HolderNumChange` decimal(13,0) DEFAULT NULL COMMENT '股东户数增减',
  `HolderNumChangeRate` decimal(10,2) DEFAULT NULL COMMENT '增减比例',
  `RangeChangeRate` decimal(10,2) DEFAULT NULL COMMENT '期间股价涨跌幅',
  `HolderAvgCapitalisation` decimal(13,2) DEFAULT NULL COMMENT '户均市值',
  `HolderAvgStockQuantity` decimal(13,0) DEFAULT NULL COMMENT '户均持股',
  `TotalCapitalisation` decimal(15,2) DEFAULT NULL COMMENT '总市值',
  `CapitalStock` decimal(13,0) DEFAULT NULL COMMENT '总股本',
  `CapitalStockChange` decimal(13,0) DEFAULT NULL COMMENT '总股本变动',
  `CapitalStockChangeEvent` varchar(45) DEFAULT NULL COMMENT '总股本变动原因',
  `NoticeDate` date DEFAULT NULL COMMENT '公告日期',
  `ClosePrice` decimal(10,2) DEFAULT NULL COMMENT '收盘价',
  `PreviousHolderNum` decimal(13,0) DEFAULT NULL COMMENT '上期股东数',
  `PreviousEndDate` date DEFAULT NULL COMMENT '上期截止日期',
  `temp` decimal(13,2) DEFAULT '0.00',
  `nextClose` decimal(13,2) DEFAULT NULL COMMENT '下季收盘价',
  `nextZF` decimal(13,2) DEFAULT NULL COMMENT '到下季时的涨幅',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`stockCode`,`EndDate`),
  UNIQUE KEY `index3` (`EndDate`,`stockCode`)
) ENGINE=InnoDB AUTO_INCREMENT=263589 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jgcc`
--

DROP TABLE IF EXISTS `jgcc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jgcc` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCode` varchar(6) DEFAULT NULL,
  `RDate` date DEFAULT NULL COMMENT '统计截止日期',
  `lx` decimal(10,0) DEFAULT NULL,
  `Count` decimal(13,0) DEFAULT NULL COMMENT '机构家数',
  `CGChange` varchar(10) DEFAULT NULL COMMENT '持股变化：减持（-1）、持平（0）、增持（1）',
  `ShareHDNum` decimal(13,0) DEFAULT NULL COMMENT '持股总数',
  `VPosition` decimal(15,2) DEFAULT NULL COMMENT '持股市值',
  `TabRate` decimal(13,2) DEFAULT NULL COMMENT '占总股本比例',
  `LTZB` decimal(13,2) DEFAULT NULL COMMENT '占流通股比例',
  `ShareHDNumChange` decimal(13,0) DEFAULT NULL COMMENT '持股变动数',
  `RateChange` decimal(13,2) DEFAULT NULL COMMENT '变动比例',
  `LTZBChange` decimal(13,2) DEFAULT NULL COMMENT '变动占流通股的比例',
  `ClosePrice` decimal(13,2) DEFAULT NULL COMMENT '报告期收盘价',
  `ChangeValue` decimal(13,2) DEFAULT NULL COMMENT '持仓变动市值',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`SCode`,`RDate`,`lx`),
  KEY `index3` (`RDate`,`SCode`)
) ENGINE=InnoDB AUTO_INCREMENT=377163 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jgcg`
--

DROP TABLE IF EXISTS `jgcg`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jgcg` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stockCode` varchar(6) NOT NULL,
  `EndDate` date NOT NULL COMMENT '统计截止日期',
  `endTradeDate` date DEFAULT NULL,
  `ClosePrice` decimal(13,2) DEFAULT '0.00',
  `LTTotalHDNum` decimal(15,0) DEFAULT '0' COMMENT '流通总股本',
  `Count0` decimal(13,0) DEFAULT NULL COMMENT '机构家数',
  `CGChange0` varchar(6) DEFAULT NULL COMMENT '持股变化：减持（-1）、持平（0）、增持（1）',
  `ShareHDNum0` decimal(13,0) DEFAULT NULL COMMENT '持股总数',
  `VPosition0` decimal(15,0) DEFAULT NULL COMMENT '持股市值',
  `TabRate0` decimal(13,2) DEFAULT NULL COMMENT '占总股本比例',
  `LTZB0` decimal(13,2) DEFAULT NULL COMMENT '占流通股比例',
  `ShareHDNumChange0` decimal(13,0) DEFAULT NULL COMMENT '持股变动数',
  `RateChange0` decimal(13,2) DEFAULT NULL COMMENT '变动比例',
  `LTZBChange0` decimal(13,2) DEFAULT '0.00' COMMENT '变动占流通股的比例',
  `VPositionChange0` decimal(15,0) DEFAULT '0',
  `Count1` decimal(13,0) DEFAULT '0' COMMENT '基金机构家数',
  `CGChange1` varchar(6) DEFAULT NULL COMMENT '基金持股变化：减持（-1）、新进（1）、增持（2）、持平（0）',
  `ShareHDNum1` decimal(13,0) DEFAULT '0' COMMENT '基金持股总数',
  `VPosition1` decimal(15,0) DEFAULT '0' COMMENT '基金持股市值',
  `TabRate1` decimal(13,2) DEFAULT '0.00' COMMENT '基金占总股本比例',
  `LTZB1` decimal(13,2) DEFAULT '0.00' COMMENT '基金占流通股比例',
  `ShareHDNumChange1` decimal(13,0) DEFAULT '0' COMMENT '基金持股变动数',
  `RateChange1` decimal(13,2) DEFAULT '0.00' COMMENT '基金变动比例',
  `LTZBChange1` decimal(13,2) DEFAULT '0.00' COMMENT '基金变动占流通股的比例',
  `VPositionChange1` decimal(15,0) DEFAULT NULL,
  `Count2` decimal(13,0) DEFAULT '0' COMMENT 'QFII机构家数',
  `CGChange2` varchar(6) DEFAULT NULL COMMENT 'QFII持股变化：减持（-1）、新进（1）、增持（2）、持平（0）',
  `ShareHDNum2` decimal(13,0) DEFAULT '0' COMMENT 'QFII持股总数',
  `VPosition2` decimal(15,0) DEFAULT '0' COMMENT 'QFII持股市值',
  `TabRate2` decimal(13,2) DEFAULT '0.00' COMMENT 'QFII占总股本比例',
  `LTZB2` decimal(13,2) DEFAULT '0.00' COMMENT 'QFII占流通股比例',
  `ShareHDNumChange2` decimal(13,0) DEFAULT '0' COMMENT 'QFII持股变动数',
  `RateChange2` decimal(13,2) DEFAULT '0.00' COMMENT 'QFII变动比例',
  `LTZBChange2` decimal(13,2) DEFAULT '0.00' COMMENT 'QFII变动占流通股的比例',
  `VPositionChange2` decimal(15,0) DEFAULT NULL,
  `Count3` decimal(13,0) DEFAULT '0' COMMENT '社保机构家数',
  `CGChange3` varchar(6) DEFAULT NULL COMMENT '社保持股变化：减持（-1）、新进（1）、增持（2）、持平（0）',
  `ShareHDNum3` decimal(13,0) DEFAULT '0' COMMENT '社保持股总数',
  `VPosition3` decimal(15,0) DEFAULT '0' COMMENT '社保持股市值',
  `TabRate3` decimal(13,2) DEFAULT '0.00' COMMENT '社保占总股本比例',
  `LTZB3` decimal(13,2) DEFAULT '0.00' COMMENT '社保占流通股比例',
  `ShareHDNumChange3` decimal(13,0) DEFAULT '0' COMMENT '社保持股变动数',
  `RateChange3` decimal(13,2) DEFAULT '0.00' COMMENT '社保变动比例',
  `LTZBChange3` decimal(13,2) DEFAULT '0.00' COMMENT '社保变动占流通股的比例',
  `VPositionChange3` decimal(15,0) DEFAULT NULL,
  `Count4` decimal(13,0) DEFAULT '0' COMMENT '券商机构家数',
  `CGChange4` varchar(6) DEFAULT NULL COMMENT '券商持股变化：减持（-1）、新进（1）、增持（2）、持平（0）',
  `ShareHDNum4` decimal(13,0) DEFAULT '0' COMMENT '券商持股总数',
  `VPosition4` decimal(15,0) DEFAULT '0' COMMENT '券商持股市值',
  `TabRate4` decimal(13,2) DEFAULT '0.00' COMMENT '券商占总股本比例',
  `LTZB4` decimal(13,2) DEFAULT '0.00' COMMENT '券商占流通股比例',
  `ShareHDNumChange4` decimal(13,0) DEFAULT '0' COMMENT '券商持股变动数',
  `RateChange4` decimal(13,2) DEFAULT '0.00' COMMENT '券商变动比例',
  `LTZBChange4` decimal(13,2) DEFAULT '0.00' COMMENT '券商变动占流通股的比例',
  `VPositionChange4` decimal(15,0) DEFAULT NULL,
  `Count5` decimal(13,0) DEFAULT '0' COMMENT '保险机构家数',
  `CGChange5` varchar(6) DEFAULT NULL COMMENT '保险持股变化：减持（-1）、新进（1）、增持（2）、持平（0）',
  `ShareHDNum5` decimal(13,0) DEFAULT '0' COMMENT '保险持股总数',
  `VPosition5` decimal(15,0) DEFAULT '0' COMMENT '保险持股市值',
  `TabRate5` decimal(13,2) DEFAULT '0.00' COMMENT '保险占总股本比例',
  `LTZB5` decimal(13,2) DEFAULT '0.00' COMMENT '保险占流通股比例',
  `ShareHDNumChange5` decimal(13,0) DEFAULT '0' COMMENT '保险持股变动数',
  `RateChange5` decimal(13,2) DEFAULT '0.00' COMMENT '保险变动比例',
  `LTZBChange5` decimal(13,2) DEFAULT '0.00' COMMENT '保险变动占流通股的比例',
  `VPositionChange5` decimal(15,0) DEFAULT NULL,
  `Count6` decimal(13,0) DEFAULT '0' COMMENT '信托机构家数',
  `CGChange6` varchar(6) DEFAULT NULL COMMENT '信托持股变化：减持（-1）、新进（1）、增持（2）、持平（0）',
  `ShareHDNum6` decimal(13,0) DEFAULT '0' COMMENT '信托持股总数',
  `VPosition6` decimal(15,0) DEFAULT '0' COMMENT '信托持股市值',
  `TabRate6` decimal(13,2) DEFAULT '0.00' COMMENT '信托占总股本比例',
  `LTZB6` decimal(13,2) DEFAULT '0.00' COMMENT '信托占流通股比例',
  `ShareHDNumChange6` decimal(13,0) DEFAULT '0' COMMENT '信托持股变动数',
  `RateChange6` decimal(13,2) DEFAULT '0.00' COMMENT '信托变动比例',
  `LTZBChange6` decimal(13,2) DEFAULT '0.00' COMMENT '信托变动占流通股的比例',
  `VPositionChange6` decimal(15,0) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`stockCode`,`EndDate`)
) ENGINE=InnoDB AUTO_INCREMENT=27419361 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jgcgmx`
--

DROP TABLE IF EXISTS `jgcgmx`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jgcgmx` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCode` varchar(10) DEFAULT NULL COMMENT '股票代码',
  `RDate` date DEFAULT NULL,
  `SHCode` varchar(10) DEFAULT NULL COMMENT '机构代码',
  `TypeCode` decimal(8,0) DEFAULT NULL COMMENT '机构类型',
  `indtCode` varchar(10) DEFAULT NULL COMMENT '机构管理公司代码',
  `ShareHDNum` decimal(15,0) DEFAULT NULL COMMENT '持股',
  `Vposition` decimal(15,0) DEFAULT NULL COMMENT '市值',
  `TabRate` decimal(10,2) DEFAULT NULL COMMENT '占总股本比例',
  `TabProRate` decimal(10,2) DEFAULT NULL COMMENT '占流通股比例',
  PRIMARY KEY (`id`),
  UNIQUE KEY `SCode_UNIQUE` (`SCode`,`RDate`,`SHCode`)
) ENGINE=InnoDB AUTO_INCREMENT=1848911 DEFAULT CHARSET=utf8 COMMENT='机构持股明细';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `jgcgmx_Q32`
--

DROP TABLE IF EXISTS `jgcgmx_Q32`;
/*!50001 DROP VIEW IF EXISTS `jgcgmx_Q32`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `jgcgmx_Q32` (
  `scode` tinyint NOT NULL,
  `Q3` tinyint NOT NULL,
  `Q2` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `jigou`
--

DROP TABLE IF EXISTS `jigou`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jigou` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SHCode` varchar(10) NOT NULL COMMENT '机构代码',
  `SHName` varchar(45) DEFAULT NULL COMMENT '机构名称',
  `lx` decimal(8,0) DEFAULT NULL COMMENT '1-基金\n2-QFII\n3-社保\n4-券商\n5-保险\n6-信托',
  `IndtCode` varchar(10) DEFAULT NULL COMMENT '管理公司代码',
  `InstSName` varchar(45) DEFAULT NULL COMMENT '管理公司名称',
  `VPosition` decimal(15,0) DEFAULT NULL COMMENT '机构市值规模（曾经最大市值）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `SHCode_UNIQUE` (`SHCode`)
) ENGINE=InnoDB AUTO_INCREMENT=15873 DEFAULT CHARSET=utf8 COMMENT='机构';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stock`
--

DROP TABLE IF EXISTS `stock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stock` (
  `stockCode` varchar(6) NOT NULL,
  `stockName` varchar(45) DEFAULT NULL,
  `market` int(11) DEFAULT NULL,
  PRIMARY KEY (`stockCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `stocksOfBlock`
--

DROP TABLE IF EXISTS `stocksOfBlock`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `stocksOfBlock` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `blkUcode` varchar(10) DEFAULT NULL,
  `stockCode` varchar(10) DEFAULT NULL,
  `marketid` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ndx` (`blkUcode`,`stockCode`)
) ENGINE=InnoDB AUTO_INCREMENT=290136 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Final view structure for view `jgcgmx_Q32`
--

/*!50001 DROP TABLE IF EXISTS `jgcgmx_Q32`*/;
/*!50001 DROP VIEW IF EXISTS `jgcgmx_Q32`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`chenyl`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `jgcgmx_Q32` AS select `jgcgmx`.`SCode` AS `scode`,sum(if((`jgcgmx`.`RDate` = '2017-09-30'),`jgcgmx`.`ShareHDNum`,0)) AS `Q3`,sum(if((`jgcgmx`.`RDate` = '2017-06-30'),`jgcgmx`.`ShareHDNum`,0)) AS `Q2` from `jgcgmx` where ((`jgcgmx`.`RDate` = '2017-09-30') or (`jgcgmx`.`RDate` = '2017-06-30')) group by `jgcgmx`.`SCode` order by `jgcgmx`.`SCode` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-08-06 12:04:59

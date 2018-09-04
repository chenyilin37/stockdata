CREATE DATABASE  IF NOT EXISTS `stockdata` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `stockdata`;
-- MySQL dump 10.13  Distrib 5.7.17, for macos10.12 (x86_64)
--
-- Host: 192.168.1.8    Database: stockdata
-- ------------------------------------------------------
-- Server version	5.7.23

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
-- Table structure for table `fhsg`
--

DROP TABLE IF EXISTS `fhsg`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fhsg` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `SCode` varchar(10) NOT NULL,
  `Rdate` date NOT NULL COMMENT ' 报告期',
  `SZZBL` decimal(10,2) DEFAULT NULL COMMENT ' 送转总比例',
  `SGBL` decimal(10,2) DEFAULT NULL COMMENT ' 送股比例',
  `ZGBL` decimal(10,2) DEFAULT NULL COMMENT ' 转股比例',
  `XJFH` decimal(10,2) DEFAULT NULL COMMENT ' 现金分红',
  `GXL` decimal(10,2) DEFAULT NULL COMMENT ' 股息率',
  `YAGGR` date DEFAULT NULL COMMENT ' 预案公告日',
  `GQDJR` date DEFAULT NULL COMMENT ' 股权登记日',
  `CQCXR` date DEFAULT NULL COMMENT '股权除息日',
  `YAGGRHSRZF` decimal(10,2) DEFAULT NULL COMMENT ' 预案公告日后10日涨幅',
  `GQDJRQSRZF` decimal(10,2) DEFAULT NULL COMMENT '股权登记日后10日涨幅',
  `CQCXRHSSRZF` decimal(10,2) DEFAULT NULL COMMENT '股权除息日后10日涨幅',
  `YCQTS` decimal(15,2) DEFAULT NULL COMMENT '未知',
  `TotalEquity` decimal(15,2) DEFAULT NULL COMMENT ' 总股本',
  `EarningsPerShare` decimal(10,2) DEFAULT NULL COMMENT ' 每股收益',
  `NetAssetsPerShare` decimal(10,2) DEFAULT NULL COMMENT ' 每股净资产',
  `MGGJJ` decimal(10,2) DEFAULT NULL COMMENT ' 每股公积金',
  `MGWFPLY` decimal(10,2) DEFAULT NULL COMMENT '每股未分配利润',
  `JLYTBZZ` decimal(10,2) DEFAULT NULL COMMENT '净利润同比增长率',
  `ResultsbyDate` date DEFAULT NULL COMMENT '业绩披露日期',
  `ProjectProgress` varchar(45) DEFAULT NULL COMMENT '方案进度',
  `AllocationPlan` varchar(128) DEFAULT NULL COMMENT ' 分配预案',
  `reAdjust` decimal(10,2) DEFAULT NULL COMMENT '除权调整价格',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index2` (`SCode`,`Rdate`),
  KEY `index3` (`Rdate`,`SCode`)
) ENGINE=InnoDB AUTO_INCREMENT=27173 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-09-03 18:08:06

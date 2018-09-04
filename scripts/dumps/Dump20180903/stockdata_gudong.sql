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
-- Table structure for table `gudong`
--

DROP TABLE IF EXISTS `gudong`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `gudong` (
  `SHCode` varchar(36) NOT NULL COMMENT '股东代码',
  `SHName` varchar(128) DEFAULT NULL COMMENT '股东名称',
  `gdlx` varchar(20) DEFAULT NULL COMMENT '股东类型',
  `lxdm` decimal(8,0) DEFAULT NULL COMMENT '1-基金\n2-QFII\n3-社保\n4-券商\n5-保险\n6-信托\n7-金融、（此编号及以下，自编，带确认）\n8-投资公司、\n9-财务公司、\n10-集合理财计划、\n11-个人、\n99-其他',
  `niu` int(11) DEFAULT '0' COMMENT '是否实力机构',
  `VPosition` decimal(15,0) DEFAULT NULL COMMENT '机构市值规模（曾经最大市值）',
  `IndtCode` varchar(36) DEFAULT NULL COMMENT '管理公司编号',
  `IndtName` varchar(126) DEFAULT NULL COMMENT '管理公司名称',
  PRIMARY KEY (`SHCode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='股东';
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

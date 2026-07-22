SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- 1. 创建用户表 (t_user)
-- ----------------------------
DROP TABLE IF EXISTS `t_user`;
CREATE TABLE `t_user` (
  `USER_ID` bigint NOT NULL AUTO_INCREMENT,
  `USERNAME` varchar(64) NOT NULL,
  `PASSWORD` varchar(64) NOT NULL,
  `NICK_NAME` varchar(50) NOT NULL,
  `USER_TYPE` int NOT NULL,
  `STATUS` char(1) NOT NULL,
  `SEX` char(1) DEFAULT NULL,
  `EMAIL` varchar(64) DEFAULT NULL,
  `MOBILE` varchar(20) DEFAULT NULL,
  `DESCRIPTION` varchar(200) DEFAULT NULL,
  `CREATE_TIME` datetime NOT NULL,
  `MODIFY_TIME` datetime DEFAULT NULL,
  `LAST_LOGIN_TIME` datetime DEFAULT NULL,
  PRIMARY KEY (`USER_ID`),
  UNIQUE KEY `UN_USERNAME` (`USERNAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Bootstrap administrator credential comes from ${MYSQL_PASSWORD}; no default is stored in Git.
-- ----------------------------
BEGIN;
INSERT INTO `t_user` VALUES (100000, 'admin', '38b99182bb1665a3d463b28b704c7c8b8743179262e3d305609423c7', 'Administrator', 1, '1', '0', NULL, NULL, 'Super Admin', NOW(), NOW(), NULL);
COMMIT;

-- ----------------------------
-- 3. 创建团队表 (t_team) - 必须有，否则登录后报错
-- ----------------------------
DROP TABLE IF EXISTS `t_team`;
CREATE TABLE `t_team` (
  `TEAM_ID` bigint NOT NULL AUTO_INCREMENT,
  `TEAM_NAME` varchar(50) NOT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  `CREATE_TIME` datetime NOT NULL,
  `MODIFY_TIME` datetime DEFAULT NULL,
  PRIMARY KEY (`TEAM_ID`),
  UNIQUE KEY `UN_TEAM_NAME` (`TEAM_NAME`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `t_team` VALUES (100000, 'default', 'Default Team', NOW(), NOW());

-- ----------------------------
-- 4. 关联表 (t_user_role)
-- ----------------------------
DROP TABLE IF EXISTS `t_user_role`;
CREATE TABLE `t_user_role` (
  `ID` bigint NOT NULL AUTO_INCREMENT,
  `USER_ID` bigint DEFAULT NULL,
  `ROLE_ID` bigint DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `UN_USER_ROLE_ID` (`USER_ID`,`ROLE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `t_user_role` VALUES (100000, 100000, 100000);

-- ----------------------------
-- 5. 角色表 (t_role)
-- ----------------------------
DROP TABLE IF EXISTS `t_role`;
CREATE TABLE `t_role` (
  `ROLE_ID` bigint NOT NULL AUTO_INCREMENT,
  `ROLE_NAME` varchar(50) NOT NULL,
  `CREATE_TIME` datetime NOT NULL,
  `MODIFY_TIME` datetime DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ROLE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `t_role` VALUES (100000, 'admin', NOW(), NOW(), 'Super Admin');

SET FOREIGN_KEY_CHECKS = 1;

-- Database: DB029
-- Table for Index Scan Testing

DROP DATABASE IF EXISTS db029;
CREATE DATABASE db029;
USE db029;

CREATE TABLE `big_tbl` (
  `pk` int NOT NULL,
  `val_1` int DEFAULT NULL,
  `val_2` int DEFAULT NULL,
  `val_3` double DEFAULT NULL,
  `content` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `idx_val` (`val_1`,`val_2`)
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (0, 0, 0, 0, '0_0_0.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (1, 1, 2, 3, '1_2_3.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (2, 2, 4, 6, '2_4_6.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (3, 3, 6, 9, '3_6_9.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (4, 4, 8, 12, '4_8_12.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (5, 5, 10, 15, '5_10_15.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (6, 6, 12, 18, '6_12_18.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (7, 7, 14, 21, '7_14_21.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (8, 8, 16, 24, '8_16_24.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (9, 9, 18, 27, '9_18_27.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (10, 10, 20, 30, '10_20_30.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (11, 11, 22, 33, '11_22_33.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (12, 12, 24, 36, '12_24_36.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (13, 13, 26, 39, '13_26_39.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (14, 14, 28, 42, '14_28_42.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (15, 15, 30, 45, '15_30_45.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (16, 16, 32, 48, '16_32_48.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (17, 17, 34, 51, '17_34_51.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (18, 18, 36, 54, '18_36_54.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (19, 19, 38, 57, '19_38_57.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (20, 20, 40, 60, '20_40_60.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (21, 21, 42, 63, '21_42_63.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (22, 22, 44, 66, '22_44_66.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (23, 23, 46, 69, '23_46_69.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (24, 24, 48, 72, '24_48_72.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (25, 25, 50, 75, '25_50_75.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (26, 26, 52, 78, '26_52_78.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (27, 27, 54, 81, '27_54_81.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (28, 28, 56, 84, '28_56_84.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (29, 29, 58, 87, '29_58_87.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (30, 30, 60, 90, '30_60_90.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (31, 31, 62, 93, '31_62_93.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (32, 32, 64, 96, '32_64_96.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (33, 33, 66, 99, '33_66_99.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (34, 34, 68, 102, '34_68_102.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (35, 35, 70, 105, '35_70_105.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (36, 36, 72, 108, '36_72_108.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (37, 37, 74, 111, '37_74_111.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (38, 38, 76, 114, '38_76_114.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (39, 39, 78, 117, '39_78_117.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (40, 40, 80, 120, '40_80_120.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (41, 41, 82, 123, '41_82_123.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (42, 42, 84, 126, '42_84_126.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (43, 43, 86, 129, '43_86_129.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (44, 44, 88, 132, '44_88_132.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (45, 45, 90, 135, '45_90_135.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (46, 46, 92, 138, '46_92_138.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (47, 47, 94, 141, '47_94_141.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (48, 48, 96, 144, '48_96_144.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (49, 49, 98, 147, '49_98_147.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (50, 50, 100, 150, '50_100_150.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (51, 51, 102, 153, '51_102_153.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (52, 52, 104, 156, '52_104_156.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (53, 53, 106, 159, '53_106_159.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (54, 54, 108, 162, '54_108_162.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (55, 55, 110, 165, '55_110_165.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (56, 56, 112, 168, '56_112_168.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (57, 57, 114, 171, '57_114_171.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (58, 58, 116, 174, '58_116_174.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (59, 59, 118, 177, '59_118_177.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (60, 60, 120, 180, '60_120_180.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (61, 61, 122, 183, '61_122_183.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (62, 62, 124, 186, '62_124_186.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (63, 63, 126, 189, '63_126_189.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (64, 64, 128, 192, '64_128_192.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (65, 65, 130, 195, '65_130_195.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (66, 66, 132, 198, '66_132_198.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (67, 67, 134, 201, '67_134_201.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (68, 68, 136, 204, '68_136_204.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (69, 69, 138, 207, '69_138_207.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (70, 70, 140, 210, '70_140_210.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (71, 71, 142, 213, '71_142_213.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (72, 72, 144, 216, '72_144_216.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (73, 73, 146, 219, '73_146_219.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (74, 74, 148, 222, '74_148_222.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (75, 75, 150, 225, '75_150_225.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (76, 76, 152, 228, '76_152_228.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (77, 77, 154, 231, '77_154_231.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (78, 78, 156, 234, '78_156_234.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (79, 79, 158, 237, '79_158_237.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (80, 80, 160, 240, '80_160_240.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (81, 81, 162, 243, '81_162_243.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (82, 82, 164, 246, '82_164_246.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (83, 83, 166, 249, '83_166_249.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (84, 84, 168, 252, '84_168_252.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (85, 85, 170, 255, '85_170_255.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (86, 86, 172, 258, '86_172_258.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (87, 87, 174, 261, '87_174_261.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (88, 88, 176, 264, '88_176_264.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (89, 89, 178, 267, '89_178_267.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (90, 90, 180, 270, '90_180_270.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (91, 91, 182, 273, '91_182_273.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (92, 92, 184, 276, '92_184_276.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (93, 93, 186, 279, '93_186_279.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (94, 94, 188, 282, '94_188_282.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (95, 95, 190, 285, '95_190_285.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (96, 96, 192, 288, '96_192_288.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (97, 97, 194, 291, '97_194_291.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (98, 98, 196, 294, '98_196_294.0');
INSERT INTO big_tbl (pk, val_1, val_2, val_3, content) VALUES (99, 99, 198, 297, '99_198_297.0');

CREATE TABLE `tiny_tbl` (
  `pk` int NOT NULL,
  `val_1` int DEFAULT NULL,
  `val_2` int DEFAULT NULL,
  `val_3` double DEFAULT NULL,
  `content` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  KEY `idx_val` (`val_1`,`val_2`)
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO tiny_tbl (pk, val_1, val_2, val_3, content) VALUES (0, 0, 0, 0, '0_0_0.0');
INSERT INTO tiny_tbl (pk, val_1, val_2, val_3, content) VALUES (1, 1, 2, 3, '1_2_3.0');
INSERT INTO tiny_tbl (pk, val_1, val_2, val_3, content) VALUES (2, 2, 4, 6, '2_4_6.0');
INSERT INTO tiny_tbl (pk, val_1, val_2, val_3, content) VALUES (3, 3, 6, 9, '3_6_9.0');
INSERT INTO tiny_tbl (pk, val_1, val_2, val_3, content) VALUES (4, 4, 8, 12, '4_8_12.0');
INSERT INTO tiny_tbl (pk, val_1, val_2, val_3, content) VALUES (5, 5, 10, 15, '5_10_15.0');

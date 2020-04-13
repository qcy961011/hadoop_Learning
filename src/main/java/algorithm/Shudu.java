package algorithm;

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Shudu {
    //日志记录器
    private static Logger logger = Logger.getLogger("SuDoKu");

    private static void display(int a[][]) {
        for (int i = 0; i < 9; i++) {
            String temp = "";
            for (int j = 0; j < 9; j++) {
                temp += "\t" + a[i][j];
            }
            System.out.println(temp);
//            logger.info(temp);
        }
    }

    /**
     * 判断数组是否合法
     */
    public static boolean isLegal(int a[][]) {
//判断横向、纵向
        for (int i = 0; i < 9; i++) {
            for (int j = 0; j < 9; j++) {
                for (int k = j + 1; k < 9; k++) {
//判断横向是否符合要求
                    if (a[i][j] != 0 && a[i][k] == a[i][j]) {
                        return false;
                    }
//判断纵向是否符合要求
                    if (a[j][i] != 0 && a[k][i] == a[j][i]) {
                        return false;
                    }
                }
            }
        }
//判断九宫
        int flag = 0;
        while (flag < 6) {
            for (int i = 0 + flag; i < 3 + flag; i++) {
                for (int j = 0 + flag; j < 3 + flag; j++) {
                    for (int k = i + 1; k < 3 + flag; k++) {
                        for (int l = j + 1; l < 3 + flag; l++) {
                            if (a[i][j] != 0 &&
                                    a[i][j] == a[i][l] &&
                                    a[i][j] == a[k][j] &&
                                    a[i][j] == a[k][l]) {
                                return false;
                            }
                        }
                    }
                }
            }
            flag = flag + 3;
        }
        return true;
    }

    //测试
    public static void main(String[] args) {
        int[][] a = {
                {9, 8, 5, 7, 6, 2, 1, 3, 4},
                {2, 6, 7, 1, 3, 4, 5, 8, 9},
                {3, 1, 4, 8, 9, 5, 7, 6, 2},
                {8, 3, 2, 9, 7, 6, 4, 5, 1},
                {1, 7, 6, 4, 5, 3, 2, 9, 8},
                {5, 4, 9, 2, 1, 8, 6, 7, 3},
                {6, 2, 1, 3, 8, 7, 9, 4, 5},
                {4, 5, 3, 6, 2, 9, 8, 1, 7},
                {7, 9, 8, 5, 4, 1, 3, 2, 6}
        };
        boolean flag = isLegal(a);
        if (flag) {
            logger.info("\t\t\t\t以下数组是数独");
            display(a);
        } else {
            logger.info("\t\t\t\t以下数组不是数独");
            display(a);
        }
    }
}

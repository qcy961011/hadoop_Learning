package com.hive.conntrllerText;

import org.apache.hadoop.hive.cli.CliDriver;

public class HiveDiver {

    public static void main(String[] args) throws Exception {
        System.setProperty("jline.WindowsTerminal.directConsole","false");
        int ret = new CliDriver().run(args);
        System.exit(ret);

    }
}

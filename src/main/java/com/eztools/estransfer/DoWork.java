package com.eztools.estransfer;

import com.eztools.estransfer.common.ESSourceClient;
import com.eztools.estransfer.common.ESTargetClient;
import com.eztools.estransfer.common.EsSourceConfig;
import com.eztools.estransfer.common.EsTargetConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Created by liChenYu on 2018/6/17
 */
@Component
public class DoWork implements CommandLineRunner {
    private static Logger s_logger = LoggerFactory.getLogger(DoWork.class);

    @Autowired
    EsSourceConfig esSourceConfig;

    @Autowired
    EsTargetConfig esTargetConfig;

    @Autowired
    TransferService transferService;

    @Override
    public void run(String... strings) {
        ESSourceClient.getHightClient(esSourceConfig);
        s_logger.info(esSourceConfig.toString());

        ESTargetClient.getHightClient(esTargetConfig);
        s_logger.info(esTargetConfig.toString());

        s_logger.info("######## init all es connections done!");

        transferService.doTransfer(esSourceConfig, esTargetConfig);

        System.exit(0);
    }
}

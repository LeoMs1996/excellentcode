package com.mogujie.ncrm.dispute.thread.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 分片工具(用于将一个大任务才分成多个子任务来并行执行)
 * 由于#sharding方法没有参数，所以本工具适用于方法内部或能取到上下文信息的地方操作
 * @param <TP>  单个任务的参数类型
 * @param <TR>  单个任务的返回值类型
 * @param <R>   最终结果类型
 *
 * @author laibao
 */
public interface ShardingTaskScheduler<TP ,TR , R> {

    Logger logger = LoggerFactory.getLogger(ShardingTaskScheduler.class);

    /**
     * 执行分片，切分成单个任务请求参数列表
     * @return
     */
    List<TP> sharding();

    /**
     * 执行单个任务
     * @param taskParam
     * @return
     */
    TR executeSingleTask(TP taskParam);

    /**
     * 合并所有任务的结果
     * @param trList
     * @return
     */
    R mergeTaskResult(List<TR> trList);

    /**
     * 执行入口
     * 执行顺序
     *      #sharding -> 并行执行#executeSingleTask -> 合并结果集
     *
     * @param executorService
     * @return
     */
    default R execute(ExecutorService executorService){
        //先根据context分片出每个task的参数
        List<TP> taskParams = sharding();
        //构建每个Task
        List<Future<TR>> futureList = new LinkedList();
        taskParams.forEach(taskParam -> {
                    Future future = executorService.submit(() ->
                                    executeSingleTask(taskParam)
                    );
                    futureList.add(future);
                }
        );
        //提取每个task的结果
        List<TR> trList = new LinkedList();
        futureList.forEach(future -> {
            TR tr = null;
            try {
                tr = future.get();
            } catch (Throwable e) {
                logger.error(e.getMessage() ,e);
            }
            if(tr != null){
                trList.add(tr);
            }
        });
        //合并每个task的结果
        return mergeTaskResult(trList);
    }

}

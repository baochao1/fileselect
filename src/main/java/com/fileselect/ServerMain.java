package com.fileselect;

import com.util.PropertiesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ServerMain {
    private static Logger logger = LoggerFactory.getLogger(ServerMain.class);
    private static PropertiesLoader loader = new PropertiesLoader("config.properties");
    private static Executor executor = Executors.newFixedThreadPool(200);
    private static Random random = new Random();

    public static void main(String[] args) {
        //配置  挑选的音频文件的  txt文件
        long selectFileStart = System.currentTimeMillis();
        List<String> selectFileNames = getSelectFileNames();
        long selectFileEnd = System.currentTimeMillis();
        logger.info("获取筛选文件花费时间{}", selectFileEnd - selectFileStart);
        //将待处理的文件夹中的所有数据取出，带上全路径。

        List<String> audioPathList = getAudioPath();
        long allOriginalEnd = System.currentTimeMillis();
        logger.info("获取所有原始文件列表花费时间{}", allOriginalEnd - selectFileEnd);

        //根据上面两个List挑选出需要筛选出的文件
        if (!selectFileNames.isEmpty() && !audioPathList.isEmpty()) {

            List<String> filterFiles = filterFile(selectFileNames, audioPathList);
            long finalFilterFileEnd = System.currentTimeMillis();
            logger.info("获取筛选文件的位置信息花费时间{}, 文件size为 {}", finalFilterFileEnd - allOriginalEnd, filterFiles.size());
            logger.info("去除重复数据之后的带筛选数据大小是 {}", new HashSet<>(filterFiles).size());


            List<List<String>> spiltList = buildSpiltList(filterFiles, 100);
            long spiltListEnd = System.currentTimeMillis();
            logger.info("分割待迁移文件list花费时间{}, 分割list成 {} 份", spiltListEnd - finalFilterFileEnd, spiltList.size());
            logger.info("分割前的总数量{}, 分割后的总数量{}", filterFiles.size(), countListSize(spiltList));
            copyOriginalFileToSpecialDirectory(spiltList);
        } else {
            logger.info("暂无可筛选数据，可能配置 文件或者音频文件夹数据为空。");
        }
    }

    private static int countListSize(List<List<String>> spiltList) {
        int count = 0;
        for (List<String> strings : spiltList) {
            count += strings.size();
        }
        return count;
    }

    private static List<String> filterFile(List<String> selectFileNames, List<String> audioPathList) {
        List<List<String>> spiltList = buildSpiltList(selectFileNames, 25);
        logger.info("过滤文件，将将要筛选的文件分割多段list，分割前总数量{}, 分割后总数量{}", selectFileNames.size(), countListSize(spiltList));
        List<String> list = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(spiltList.size());
        for (List<String> item :
                spiltList) {
            executor.execute(new FilterFiles(item, audioPathList, list, latch));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("latch wait方法异常", e);
            Thread.currentThread().interrupt();
        }
        return list;
    }

    private static List<List<String>> buildSpiltList(List<String> filterFiles, int spiltSize) {
        List<List<String>> list = new ArrayList<>();
        List<String> subList = new ArrayList<>();
        for (int i = 0; i < filterFiles.size(); i++) {
            if (i == 0 || i % spiltSize > 0) {
                subList.add(filterFiles.get(i));
            } else {
                subList.add(filterFiles.get(i));
                List<String> copyList = new ArrayList<>(subList);
                list.add(copyList);
                subList.clear();
            }
        }
        list.add(subList);
        return list;
    }

    private static void copyOriginalFileToSpecialDirectory(List<List<String>> filterFiles) {
        String destinationPath = loader.getProperty("destination_file_name");
        long copyStart = System.currentTimeMillis();
        logger.info("拷贝文件开始");
        final CountDownLatch copyLatch = new CountDownLatch(filterFiles.size());
        for (List<String> filterFile : filterFiles) {
            executor.execute(new CopyFileToDestinationTask(filterFile, destinationPath, copyLatch));
        }
        try {
            copyLatch.await();
        } catch (InterruptedException e) {
            logger.info("latch wait方法异常", e);
            Thread.currentThread().interrupt();
        }
        long copyEnd = System.currentTimeMillis();
        logger.info("拷贝结束，花费时间{}", copyEnd - copyStart);
    }

    private static List<String> getAudioPath() {
        List<String> list = new ArrayList<>();
        String selectFileName = loader.getProperty("file_location");
        File file = new File(selectFileName);
        getAllOriginalFileList(file, list);
        logger.info("待筛选的数据的总数{}", list.size());
        return list;
    }

    private static void getAllOriginalFileList(File file, List<String> list) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null) {
                return;
            }
            for (File item : files) {
                if (item.isDirectory()) {
                    getAllOriginalFileList(item, list);
                } else {
                    list.add(item.getAbsolutePath());
                }
            }
        }

    }

    private static List<String> getSelectFileNames() {
        List<String> list = new ArrayList<>();
        String selectFileName = loader.getProperty("select_file_list");
        File file = new File(selectFileName);
        if (file.isFile()) {
            try (FileInputStream inputStream = new FileInputStream(file);
                 InputStreamReader ir = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(ir)) {
                while (true) {
                    String line = reader.readLine();
                    if (line != null) {
                        list.add(line.trim());
//                        logger.info("需要筛选出的文件{}", line);
                    } else {
                        break;
                    }
                }
            } catch (IOException e) {
                logger.error("读取文件出错", e);
            }
        }
        logger.info("需要筛选出的文件总数  {}", list.size());
        return list;
    }

    private static String getFileName(String fullPathName) {
        String[] content = fullPathName.split("[\\\\,/]");
        int length = content.length;
        return content[length - 1];
    }


    static class CopyFileToDestinationTask implements Runnable {
        List<String> fileList;
        String destination;
        CountDownLatch copyLatch;

        CopyFileToDestinationTask(List<String> fileList, String destination, CountDownLatch copyLatch) {
            this.destination = destination;
            this.fileList = fileList;
            this.copyLatch = copyLatch;
        }

        @Override
        public void run() {
            int rand = random.nextInt();
//            logger.info("开始执行拷贝任务分割任务list   线程标识{}", rand);
            try {
                for (String file : fileList) {
                    String fileName = getFileName(file);
                    String cmd = "cmd /c copy " + file + " " + destination + "\\" + fileName;
                    Runtime.getRuntime().exec(cmd).waitFor();
                }
            } catch (InterruptedException | IOException e) {
                logger.error("复制文件到指定位置出错", e);

                Thread.currentThread().interrupt();
            }
            copyLatch.countDown();
//            logger.info("结束拷贝任务分割任务list   线程标识{}", rand);
        }
    }

    static class FilterFiles implements Runnable {

        List<String> selectFileList;
        List<String> waitSelectFileList;
        List<String> resultList;
        CountDownLatch latch;

        FilterFiles(List<String> selectFileList, List<String> waitSelectFileList, List<String> resultList, CountDownLatch latch) {
            this.selectFileList = selectFileList;
            this.waitSelectFileList = waitSelectFileList;
            this.resultList = resultList;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (String selectFile : selectFileList) {
                for (String origin : waitSelectFileList) {
                    String fileName = getFileName(origin);
                    if (selectFile.equals(fileName) || satisfiedSign(selectFile, fileName)) {
                        resultList.add(origin);
                    }
                }
            }
            latch.countDown();
        }

        /**
         * 如果标志  配置的筛选值匹配第三个横杠值也匹配。
         * <p>
         * 9g931q103q
         * <p>
         * ISR-03-44-nb-sbyq1-9g931q103q-utt.wav
         *
         * @param selectFile selectFile
         * @param origin     origin
         * @return 结果
         */
        private static boolean satisfiedSign(String selectFile, String origin) {
            String[] signs = origin.split("-");
           /* if (signs.length > 5) {
                String suffix = (signs[5].split("\\."))[0];
                return selectFile.equals(signs[4] + "-" + suffix);
            } else {
                return false;
            }*/
            if (signs.length > 6) {
                String suffix = (signs[6].split("\\."))[0];
                return selectFile.equals(signs[5] + "-" + suffix);
            } else {
                return false;
            }
        }
    }


}

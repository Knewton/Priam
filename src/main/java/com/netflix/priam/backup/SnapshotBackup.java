package com.netflix.priam.backup;

import java.io.File;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.CronTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.RetryableCallable;

/**
 * Task for running daily snapshots
 */
@Singleton
public class SnapshotBackup extends AbstractBackup
{
    public static String JOBNAME = "SnapshotBackup";

    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    private final MetaData metaData;

    @Inject
    public SnapshotBackup(IConfiguration config, IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory, MetaData metaData)
    {
        super(config, fs, pathFactory);
        this.metaData = metaData;
    }

    @Override
    public void execute() throws Exception
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        String snapshotName = pathFactory.get().getFormat().format(cal.getTime());
        logger.info(String.format("snapshotName: %s", snapshotName));
        try
        {
            logger.info("Starting snapshot " + snapshotName);
            takeSnapshot(snapshotName);
            // Collect all snapshot dir's under keyspace dir's
            List<AbstractBackupPath> bps = Lists.newArrayList();
            File dataDir = new File(config.getDataFileLocation());
            logger.info(String.format("SnapshotBackup.execute dataDir: %s", dataDir));
            // This logic looks broken to me since it can never find
            // dataDir/keyspaceDir/snapshot.  Try an alternative

            for (File keyspaceDir : dataDir.listFiles()) {
                File snpDir = new File(keyspaceDir, "snapshots");
                if (!snpDir.isDirectory() && !snpDir.exists()) {
                    logger.debug(String.format("doesn't exist or isn't a directory: snpDir: %s", snpDir));
                    continue;
                }
                File snapshotDir = getValidSnapshot(keyspaceDir, snpDir, snapshotName);
                logger.info(String.format("Valid snapshot found, adding snapshotDir: %s", snapshotDir));
                // Add files to this dir
                if (null != snapshotDir) {
                    bps.addAll(upload(snapshotDir, BackupFileType.SNAP));
                    logger.info(String.format("snapshotDir: %s",snapshotDir));
                }
            }
            // for (File keyspaceDir : dataDir.listFiles())
            // {
            //     logger.info(String.format("keyspaceDir: %s, class: %s, name: %s", keyspaceDir, keyspaceDir.getClass(), keyspaceDir.getPath()));
            //     for (File columnFamilyDir : keyspaceDir.listFiles())
            //     {
            //         File snpDir = new File(columnFamilyDir, "snapshots");
            //         logger.debug(String.format("Trying columnFamilyDir: %s, snpDir: %s", columnFamilyDir, snpDir));
            //         if(!snpDir.isDirectory() && !snpDir.exists()) {
            //             logger.debug(String.format("doesn't exist or isn't a directory: snpDir: %s", snpDir));
            //         }
            //         if (!isValidBackupDir(keyspaceDir, columnFamilyDir, snpDir)) {
            //             logger.debug(String.format("notvalid: ksDir: %s, cfDir: %s, snpDir: %s", keyspaceDir, columnFamilyDir, snpDir));
            //             continue;
            //         } else {
            //             logger.info(String.format("Passed the sniff test ksDir: %s, cfDir: %s, snpDir: %s", keyspaceDir, columnFamilyDir, snpDir));
            //         }
            //         File snapshotDir = getValidSnapshot(columnFamilyDir, snpDir, snapshotName);
            //         logger.info(String.format("Valid snapshot found, adding snapshotDir: %s", snapshotDir));
            //         // Add files to this dir
            //         if (null != snapshotDir) {
            //             bps.addAll(upload(snapshotDir, BackupFileType.SNAP));
            //             logger.info(String.format("snapshotDir: %s",snapshotDir));
            //         }
            //     }
            // }

            // Upload meta file
            metaData.set(bps, snapshotName);
            logger.info("Snapshot upload complete for " + snapshotName);
        }
        finally
        {
            try
            {
                clearSnapshot(snapshotName);
            }
            catch (Exception e)
            {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private File getValidSnapshot(File columnFamilyDir, File snpDir, String snapshotName)
    {
        for (File snapshotDir : snpDir.listFiles())
            if (snapshotDir.getName().matches(snapshotName)) {
                logger.debug(String.format("getValidSnapshot: OK snapshotDir: %s, cFDir: %s, snpDir: %s, snapshotName: %s", snapshotDir, columnFamilyDir, snpDir, snapshotName));
                return snapshotDir;
            } else {
                logger.debug(String.format("getValidSnapshot: Invalid: cFDir: %s, snpDir: %s, snapshotName: %s", columnFamilyDir, snpDir, snapshotName));
            }
        return null;
    }

    private void takeSnapshot(final String snapshotName) throws Exception
    {
        new RetryableCallable<Void>()
        {
            public Void retriableCall() throws Exception
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.takeSnapshot(snapshotName, null, new String[0]);
                return null;
            }
        }.call();
    }

    private void clearSnapshot(final String snapshotTag) throws Exception
    {
        new RetryableCallable<Void>()
        {
            public Void retriableCall() throws Exception
            {
                JMXNodeTool nodetool = JMXNodeTool.instance(config);
                nodetool.clearSnapshot(snapshotTag);
                return null;
            }
        }.call();
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config)
    {
        int hour = config.getBackupHour();
        return new CronTimer(hour, 1, 0);
    }
}

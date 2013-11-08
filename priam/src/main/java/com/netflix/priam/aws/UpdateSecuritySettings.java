/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.aws;

import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;

/**
 * this class will associate an Public IP's with a new instance so they can talk
 * across the regions.
 * 
 * Requirement: 1) Nodes in the same region needs to be able to talk to each
 * other. 2) Nodes in other regions needs to be able to talk to the others in
 * the other region.
 * 
 * Assumption: 1) IPriamInstanceFactory will provide the membership... and will
 * be visible across the regions 2) IMembership amazon or any other
 * implementation which can tell if the instance is part of the group (ASG in
 * amazons case).
 * 
 */
@Singleton
public class UpdateSecuritySettings extends Task
{
    public static final String JOBNAME = "Update_SG";
    public static boolean firstTimeUpdated = false;

    private static final Random ran = new Random();
    private final IMembership membership;
    private final IPriamInstanceFactory factory;

    @Inject
    public UpdateSecuritySettings(IConfiguration config, IMembership membership, IPriamInstanceFactory factory)
    {
        super(config);
        this.membership = membership;
        this.factory = factory;
    }

    /**
     * Seeds nodes execute this at the specifed interval.
     * Other nodes run only on startup.
     * Seeds in cassandra are the first node in each Availablity Zone.
     */
    @Override
    public void execute()
    {
        // if seed dont execute.
        int port_ssl = config.getSSLStoragePort();
        int port_clear = config.getStoragePort();
        List<String> acls_ssl = membership.listACL(port_ssl, port_ssl);
        List<String> acls_clear = membership.listACL(port_ssl, port_ssl);
        List<PriamInstance> instances = factory.getAllIds(config.getAppName());

        // iterate to add...
        List<String> add_ssl = Lists.newArrayList();
        List<String> add_clear = Lists.newArrayList();
        for (PriamInstance instance : factory.getAllIds(config.getAppName()))
        {
            String range = instance.getHostIP() + "/32";
            if (!acls_ssl.contains(range))
                add_ssl.add(range);
            if (!acls_clear.contains(range))
                add_clear.add(range);
        }
        if (add_ssl.size() > 0)
        {
            membership.addACL(add_ssl, port_ssl, port_ssl);
            firstTimeUpdated = true;
        }
        if (add_clear.size() > 0)
        {
            membership.addACL(add_clear, port_clear, port_clear);
            firstTimeUpdated = true;
        }

        // just iterate to generate ranges.
        List<String> currentRanges = Lists.newArrayList();
        for (PriamInstance instance : instances)
        {
            String range = instance.getHostIP() + "/32";
            currentRanges.add(range);
        }

        // iterate to remove...
        List<String> remove_ssl = Lists.newArrayList();
        for (String acl : acls_ssl)
            if (!currentRanges.contains(acl)) // if not found then remove....
                remove_ssl.add(acl);

        List<String> remove_clear = Lists.newArrayList();
        for (String acl : acls_clear)
            if (!currentRanges.contains(acl)) // if not found then remove....
                remove_clear.add(acl);

        if (remove_ssl.size() > 0)
        {
            membership.removeACL(remove_ssl, port_ssl, port_ssl);
            firstTimeUpdated = true;
        }
        if (remove_clear.size() > 0)
        {
            membership.removeACL(remove_clear, port_clear, port_clear);
            firstTimeUpdated = true;
        }
    }

    public static TaskTimer getTimer(InstanceIdentity id)
    {
        SimpleTimer return_;
        if (id.isSeed())
            return_ = new SimpleTimer(JOBNAME, 120 * 1000 + ran.nextInt(120 * 1000));
        else
            return_ = new SimpleTimer(JOBNAME);
        return return_;
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }
}

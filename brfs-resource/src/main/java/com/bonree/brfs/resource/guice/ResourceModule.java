package com.bonree.brfs.resource.guice;

import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.common.resource.ResourceCollectionInterface;
import com.bonree.brfs.resource.command.IdsCommand;
import com.bonree.brfs.resource.impl.SigarGather;
import com.google.inject.Binder;
import com.google.inject.Singleton;
import io.airlift.airline.Cli;

public class ResourceModule extends BrfsModule {
    @Override
    protected void configure(NodeType nodeType, Binder binder) {
        binder.bind(ResourceCollectionInterface.class).to(SigarGather.class).in(Singleton.class);
    }

    @Override
    public void addCommands(Cli.CliBuilder<Runnable> builder) {
        builder.withGroup("update")
               .withDescription("Version 1 upgrade version 2 compatible processing commands")
               .withDefaultCommand(IdsCommand.class)
               .withCommands(IdsCommand.class);
    }
}

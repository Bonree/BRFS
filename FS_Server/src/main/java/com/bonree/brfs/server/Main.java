/*
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

package com.bonree.brfs.server;

import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.guice.Initialization;
import com.bonree.brfs.metadata.MetadataRestoreCommand;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class Main {

    /**
     * @param args
     */
    public static void main(String[] args) {
        final Cli.CliBuilder<Runnable> builder = Cli.builder("brfs");

        builder.withDescription("BRFS command-line runner.")
               .withDefaultCommand(Help.class)
               .withCommands(Help.class, Version.class);

        List<Class<? extends Runnable>> serverCommands = Arrays.asList(
            RegionNodeCommand.class,
            DataNodeCommand.class
        );

        builder.withGroup("node")
               .withDescription("Run one of the BRFS server nodes.")
               .withDefaultCommand(Help.class)
               .withCommands(serverCommands);

        List<Class<? extends Runnable>> toolCommands = Arrays.asList(
            MetadataRestoreCommand.class
        );

        builder.withGroup("tools")
               .withDescription("Various tools for working with BRFS")
               .withDefaultCommand(Help.class)
               .withCommands(toolCommands);

        Injector baseInjector = Initialization.makeSetupInjector();
        for (BrfsModule pluginModule : baseInjector.getInstance(Key.get(new TypeLiteral<Set<BrfsModule>>() {
        }))) {
            pluginModule.addCommands(builder);
        }

        final Cli<Runnable> cli = builder.build();
        try {
            Runnable command = cli.parse(args);
            if (!(command instanceof Help)) {
                baseInjector.injectMembers(command);
            }
            command.run();
        } catch (ParseException e) {
            System.out.println("ERROR!!!!");
            System.out.println(e.getMessage());
            System.out.println("===");
            cli.parse(new String[] {"help"}).run();
            System.exit(1);
        }
    }

}

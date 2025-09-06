/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sittha.nifi.processors.dynamicListSftpProcessor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({ "files, ingest, input, list, remote, sftp, source" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class DynamicListSFTP extends AbstractProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .displayName("Hostname")
            .description("The hostname of the SFTP server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .displayName("Port")
            .description("The port of the SFTP server")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("22")
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .displayName("Username")
            .description("The username to connect to the SFTP server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("The password to connect to the SFTP server")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
            .name("Remote Path")
            .displayName("Remote Path")
            .description("The remote path to list files from")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(".")
            .build();

    public static final PropertyDescriptor IGNORE_DOTTED_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Dotted Files")
            .displayName("Ignore Dotted Files")
            .description("Whether to ignore files that start with a dot (.)")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files that are successfully listed are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed here if listing SFTP fails")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(
                HOSTNAME,
                PORT,
                USERNAME,
                PASSWORD,
                REMOTE_PATH,
                IGNORE_DOTTED_FILES);

        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ComponentLog logger = getLogger();

        FlowFile inputFlowFile = session.get();
        if (inputFlowFile == null) {
            return;
        }

        String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions(inputFlowFile).getValue();
        Integer port = Integer
                .parseInt(context.getProperty(PORT).evaluateAttributeExpressions(inputFlowFile).getValue());
        String username = context.getProperty(USERNAME).evaluateAttributeExpressions(inputFlowFile).getValue();
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(inputFlowFile).getValue();
        String remotePath = context.getProperty(REMOTE_PATH).evaluateAttributeExpressions(inputFlowFile).getValue();
        boolean ignoreDottedFiles = context.getProperty(IGNORE_DOTTED_FILES).evaluateAttributeExpressions(inputFlowFile)
                .asBoolean();

        SshClient client = SshClient.setUpDefaultClient();
        client.start();

        try (ClientSession sshSession = client.connect(username, hostname, port)
                .verify(10, TimeUnit.SECONDS)
                .getSession()) {

            sshSession.addPasswordIdentity(password);
            sshSession.auth().verify(10, TimeUnit.SECONDS);

            try (SftpClient sftp = SftpClientFactory.instance().createSftpClient(sshSession)) {

                for (SftpClient.DirEntry entry : sftp.readDir(remotePath)) {

                    if (ignoreDottedFiles && entry.getFilename().startsWith(".")) {
                        logger.info("Ignoring dotted file: {}", new Object[] { entry.getFilename() });
                        continue;
                    }

                    if (entry.getAttributes().isDirectory()) {
                        logger.info("Ignoring directory: {}", new Object[] { entry.getFilename() });
                        continue;
                    }

                    FlowFile outputFlowFile = session.create(inputFlowFile);

                    outputFlowFile = session.putAttribute(outputFlowFile, "filename", entry.getFilename());
                    outputFlowFile = session.putAttribute(outputFlowFile, "filesize",
                            String.valueOf(entry.getAttributes().getSize()));
                    outputFlowFile = session.putAttribute(outputFlowFile, "filetype",
                            String.valueOf(entry.getAttributes().getType()));
                    outputFlowFile = session.putAttribute(outputFlowFile, "path", remotePath);

                    session.transfer(outputFlowFile, REL_SUCCESS);
                    logger.info("Created FlowFile for remote entry: {}", new Object[] { entry.getFilename() });
                }

                session.remove(inputFlowFile);
            }

        } catch (Exception e) {
            getLogger().error("Failed to list SFTP directory", e);
            // ส่ง input FlowFile ไป REL_FAILURE แทน
            session.transfer(inputFlowFile, REL_FAILURE);
        } finally {
            client.stop();
        }
    }

}

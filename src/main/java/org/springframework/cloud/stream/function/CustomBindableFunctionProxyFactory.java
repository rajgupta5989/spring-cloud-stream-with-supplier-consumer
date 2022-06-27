package org.springframework.cloud.stream.function;

import com.example.rabbit.Properties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.PollableMessageSource;
import org.springframework.cloud.stream.binding.BoundTargetHolder;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;

import java.util.List;

/**
 * User: rajeshgupta
 * Date: 22/06/22
 */
public class CustomBindableFunctionProxyFactory extends BindableFunctionProxyFactory {

    private int inputCount;

    private final int outputCount;

    private final String functionDefinition;

    private final StreamFunctionProperties functionProperties;

    private final boolean pollable;

    private final Properties properties;

    @Autowired
    private GenericApplicationContext context;

    CustomBindableFunctionProxyFactory(String functionDefinition, int inputCount, int outputCount, StreamFunctionProperties functionProperties,
                                       Properties properties) {
        this(functionDefinition, inputCount, outputCount, functionProperties, false, properties);

    }

    CustomBindableFunctionProxyFactory(String functionDefinition, int inputCount, int outputCount, StreamFunctionProperties functionProperties,
                                       boolean pollable, Properties properties) {
        super(functionDefinition, inputCount, outputCount, functionProperties, pollable);
        this.inputCount = inputCount;
        this.outputCount = outputCount;
        this.functionDefinition = functionDefinition;
        this.functionProperties = functionProperties;
        this.pollable = pollable;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notEmpty(CustomBindableFunctionProxyFactory.this.bindingTargetFactories,
                "'bindingTargetFactories' cannot be empty");

        if (this.inputCount > 0) {
            for (int i = 0; i < inputCount; i++) {
                List<String> elements = this.properties.getList();
                for (String element: elements) {
                    this.createInput(element + "-" + this.buildInputNameForIndex(i));
                }
            }
        }

        if (this.outputCount > 0) {
            for (int i = 0; i < outputCount; i++) {
                this.createOutput(this.buildOutputNameForIndex(i));
            }
        }
    }

    private String buildInputNameForIndex(int index) {
        return new StringBuilder(this.functionDefinition.replace(",", "|").replace("|", ""))
                .append(FunctionConstants.DELIMITER)
                .append(FunctionConstants.DEFAULT_INPUT_SUFFIX)
                .append(FunctionConstants.DELIMITER)
                .append(index)
                .toString();
    }

    private String buildOutputNameForIndex(int index) {
        return new StringBuilder(this.functionDefinition.replace(",", "|").replace("|", ""))
                .append(FunctionConstants.DELIMITER)
                .append(FunctionConstants.DEFAULT_OUTPUT_SUFFIX)
                .append(FunctionConstants.DELIMITER)
                .append(index)
                .toString();
    }

    private void createInput(String name) {
        if (this.functionProperties.getBindings().containsKey(name)) {
            name = this.functionProperties.getBindings().get(name);
        }
        if (this.pollable) {
            PollableMessageSource pollableSource = (PollableMessageSource) getBindingTargetFactory(PollableMessageSource.class).createInput(name);
            if (context != null && !context.containsBean(name)) {
                context.registerBean(name, PollableMessageSource.class, () -> pollableSource);
            }
            this.inputHolders.put(name, new BoundTargetHolder(pollableSource, true));
        }
        else {
            this.inputHolders.put(name,
                    new BoundTargetHolder(getBindingTargetFactory(SubscribableChannel.class)
                            .createInput(name), true));
        }
    }

    private void createOutput(String name) {
        if (this.functionProperties.getBindings().containsKey(name)) {
            name = this.functionProperties.getBindings().get(name);
        }
        this.outputHolders.put(name,
                new BoundTargetHolder(getBindingTargetFactory(MessageChannel.class)
                        .createOutput(name), true));
    }
}

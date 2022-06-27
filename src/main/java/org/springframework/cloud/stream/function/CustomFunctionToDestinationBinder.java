package org.springframework.cloud.stream.function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.function.cloudevent.CloudEventMessageUtils;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.function.context.catalog.FunctionTypeUtils;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;
import org.springframework.cloud.function.context.config.RoutingFunction;
import org.springframework.cloud.function.context.message.MessageUtils;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * User: rajeshgupta
 * Date: 23/06/22
 */
public class CustomFunctionToDestinationBinder implements InitializingBean, ApplicationContextAware {

    protected final Log logger = LogFactory.getLog(getClass());

    private GenericApplicationContext applicationContext;

    private BindableProxyFactory[] bindableProxyFactories;

    private final FunctionCatalog functionCatalog;

    private final StreamFunctionProperties functionProperties;

    private final BindingServiceProperties serviceProperties;

    private final StreamBridge streamBridge;

    CustomFunctionToDestinationBinder(FunctionCatalog functionCatalog, StreamFunctionProperties functionProperties,
                                      BindingServiceProperties serviceProperties, StreamBridge streamBridge) {
        this.functionCatalog = functionCatalog;
        this.functionProperties = functionProperties;
        this.serviceProperties = serviceProperties;
        this.streamBridge = streamBridge;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (GenericApplicationContext) applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Map<String, BindableProxyFactory> beansOfType = applicationContext.getBeansOfType(BindableProxyFactory.class);
        this.bindableProxyFactories = beansOfType.values().toArray(new BindableProxyFactory[0]);
        for (BindableProxyFactory bindableProxyFactory : this.bindableProxyFactories) {
            String functionDefinition = bindableProxyFactory instanceof BindableFunctionProxyFactory
                    ? ((BindableFunctionProxyFactory) bindableProxyFactory).getFunctionDefinition()
                    : this.functionProperties.getDefinition();

            boolean shouldNotProcess = false;
            if (!(bindableProxyFactory instanceof BindableFunctionProxyFactory)) {
                Set<String> outputBindingNames = bindableProxyFactory.getOutputs();
                shouldNotProcess = !CollectionUtils.isEmpty(outputBindingNames)
                        && outputBindingNames.iterator().next().equals("applicationMetrics");
            }
            if (StringUtils.hasText(functionDefinition) && !shouldNotProcess) {
                SimpleFunctionRegistry.FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition);
                if (function != null && !function.isSupplier()) {
                    this.bindFunctionToDestinations(bindableProxyFactory, functionDefinition);
                }
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void bindFunctionToDestinations(BindableProxyFactory bindableProxyFactory, String functionDefinition) {
        this.assertBindingIsPossible(bindableProxyFactory);


        Set<String> inputBindingNames = bindableProxyFactory.getInputs();
        Set<String> outputBindingNames = bindableProxyFactory.getOutputs();

        String[] outputContentTypes = outputBindingNames.stream()
                .map(bindingName -> this.serviceProperties.getBindings().get(bindingName).getContentType())
                .toArray(String[]::new);

        SimpleFunctionRegistry.FunctionInvocationWrapper function = this.functionCatalog.lookup(functionDefinition, outputContentTypes);
        Type functionType = function.getFunctionType();
        this.assertSupportedSignatures(bindableProxyFactory, function);


        String outputDestinationName = this.determineOutputDestinationName(0, bindableProxyFactory, functionType);

        Iterator<String> iterator = inputBindingNames.iterator();

        while(iterator.hasNext()) {
            String inputDestinationName = iterator.next();
            Object inputDestination = this.applicationContext.getBean(inputDestinationName);
            if (inputDestination != null && inputDestination instanceof SubscribableChannel) {
                AbstractMessageHandler handler = createFunctionHandler(function, inputDestinationName, outputDestinationName);
                ((SubscribableChannel) inputDestination).subscribe(handler);
            }
        }

    }

    private AbstractMessageHandler createFunctionHandler(SimpleFunctionRegistry.FunctionInvocationWrapper function,
                                                         String inputChannelName, String outputChannelName) {
        ConsumerProperties consumerProperties = StringUtils.hasText(inputChannelName)
                ? this.serviceProperties.getBindingProperties(inputChannelName).getConsumer()
                : null;
        ProducerProperties producerProperties = StringUtils.hasText(outputChannelName)
                ? this.serviceProperties.getBindingProperties(outputChannelName).getProducer()
                : null;

        CustomFunctionToDestinationBinder.FunctionWrapper functionInvocationWrapper = (new CustomFunctionToDestinationBinder.FunctionWrapper(function, consumerProperties,
                producerProperties, applicationContext, this.determineTargetProtocol(outputChannelName)));

        MessagingTemplate template = new MessagingTemplate();
        template.setBeanFactory(applicationContext.getBeanFactory());
        AbstractMessageHandler handler = new AbstractMessageHandler() {
            @Override
            public void handleMessageInternal(Message<?> message) throws MessagingException {
                Object result = functionInvocationWrapper.apply((Message<byte[]>) message);
                if (result == null) {
                    logger.debug("Function execution resulted in null. No message will be sent");
                    return;
                }
                if (result instanceof Iterable) {
                    for (Object resultElement : (Iterable<?>) result) {
                        this.doSendMessage(resultElement, message);
                    }
                }
                else if (ObjectUtils.isArray(result) && !(result instanceof byte[])) {
                    for (int i = 0; i < ((Object[]) result).length; i++) {
                        this.doSendMessage(((Object[]) result)[i], message);
                    }
                }
                else {
                    this.doSendMessage(result, message);
                }
            }

            private void doSendMessage(Object result, Message<?> requestMessage) {
                if (result instanceof Message && ((Message<?>) result).getHeaders().get("spring.cloud.stream.sendto.destination") != null) {
                    String destinationName = (String) ((Message<?>) result).getHeaders().get("spring.cloud.stream.sendto.destination");
                    SubscribableChannel outputChannel = streamBridge.resolveDestination(destinationName, producerProperties);
                    if (logger.isInfoEnabled()) {
                        logger.info("Output message is sent to '" + destinationName + "' destination");
                    }
                    outputChannel.send(((Message<?>) result));
                }
                else if (StringUtils.hasText(outputChannelName)) {
                    template.send(outputChannelName, (Message<?>) result);
                }
            }

        };

        handler.setBeanFactory(this.applicationContext);
        handler.afterPropertiesSet();
        return handler;
    }

    private String determineTargetProtocol(String outputBindingName) {
        if (StringUtils.hasText(outputBindingName)) {
            String binderConfigurationName = this.serviceProperties.getBinder(outputBindingName);
            BinderFactory binderFactory = applicationContext.getBean(BinderFactory.class);
            Object binder = binderFactory.getBinder(binderConfigurationName, MessageChannel.class);
            String protocol = binder.getClass().getSimpleName().startsWith("Rabbit") ? "amqp" : "kafka";
            return protocol;
        }
        return null;
    }

    private boolean isReactiveOrMultipleInputOutput(BindableProxyFactory bindableProxyFactory, Type functionType) {
        boolean reactiveInputsOutputs = FunctionTypeUtils.isPublisher(FunctionTypeUtils.getInputType(functionType)) ||
                FunctionTypeUtils.isPublisher(FunctionTypeUtils.getOutputType(functionType));
        return isMultipleInputOutput(bindableProxyFactory) || reactiveInputsOutputs;
    }

    private String determineOutputDestinationName(int index, BindableProxyFactory bindableProxyFactory, Type functionType) {
        List<String> outputNames = new ArrayList<>(bindableProxyFactory.getOutputs());
        if (CollectionUtils.isEmpty(outputNames)) {
            outputNames = Collections.singletonList("output");
        }
        String outputDestinationName = bindableProxyFactory instanceof BindableFunctionProxyFactory
                ? ((BindableFunctionProxyFactory) bindableProxyFactory).getOutputName(index)
                : (FunctionTypeUtils.isConsumer(functionType) ? null : outputNames.get(index));
        return outputDestinationName;
    }

    private void assertBindingIsPossible(BindableProxyFactory bindableProxyFactory) {
        if (this.isMultipleInputOutput(bindableProxyFactory)) {
            Assert.isTrue(!functionProperties.isComposeTo() && !functionProperties.isComposeFrom(),
                    "Composing to/from existing Sinks and Sources are not supported for functions with multiple arguments.");
        }
    }

    private boolean isMultipleInputOutput(BindableProxyFactory bindableProxyFactory) {
        return bindableProxyFactory instanceof BindableFunctionProxyFactory
                && ((BindableFunctionProxyFactory) bindableProxyFactory).isMultiple();
    }

    private boolean isArray(Type type) {
        return type instanceof GenericArrayType || type instanceof Class && ((Class<?>) type).isArray();
    }

    private void assertSupportedSignatures(BindableProxyFactory bindableProxyFactory, SimpleFunctionRegistry.FunctionInvocationWrapper function) {
        if (this.isMultipleInputOutput(bindableProxyFactory)) {
            Assert.isTrue(!function.isConsumer(),
                    "Function '" + functionProperties.getDefinition() + "' is a Consumer which is not supported "
                            + "for multi-in/out reactive streams. Only Functions are supported");
            Assert.isTrue(!function.isSupplier(),
                    "Function '" + functionProperties.getDefinition() + "' is a Supplier which is not supported "
                            + "for multi-in/out reactive streams. Only Functions are supported");
            Assert.isTrue(!this.isArray(function.getInputType()) && !this.isArray(function.getOutputType()),
                    "Function '" + functionProperties.getDefinition() + "' has the following signature: ["
                            + function.getFunctionType() + "]. Your input and/or outout lacks arity and therefore we "
                            + "can not determine how many input/output destinations are required in the context of "
                            + "function input/output binding.");
        }
    }

    private static class FunctionWrapper implements Function<Message<byte[]>, Object> {
        private final Function function;

        private final ConsumerProperties consumerProperties;

        @SuppressWarnings("unused")
        private final ProducerProperties producerProperties;

        private final Field headersField;

        private final ConfigurableApplicationContext applicationContext;

        private final boolean isRoutingFunction;

        private final String targetProtocol;

        FunctionWrapper(Function function, ConsumerProperties consumerProperties,
                        ProducerProperties producerProperties, ConfigurableApplicationContext applicationContext, String targetProtocol) {

            isRoutingFunction = ((SimpleFunctionRegistry.FunctionInvocationWrapper) function).getTarget() instanceof RoutingFunction;
            this.applicationContext = applicationContext;
            this.function = new PartitionAwareFunctionWrapper((SimpleFunctionRegistry.FunctionInvocationWrapper) function, this.applicationContext, producerProperties);
            this.consumerProperties = consumerProperties;
            if (this.consumerProperties != null) {
                ((SimpleFunctionRegistry.FunctionInvocationWrapper) function).setSkipInputConversion(this.consumerProperties.isUseNativeDecoding());
            }
            this.producerProperties = producerProperties;
            if (this.producerProperties != null) {
                ((SimpleFunctionRegistry.FunctionInvocationWrapper) function).setSkipOutputConversion(this.producerProperties.isUseNativeEncoding());
            }
            this.headersField = ReflectionUtils.findField(MessageHeaders.class, "headers");
            this.headersField.setAccessible(true);
            this.targetProtocol = targetProtocol;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object apply(Message<byte[]> message) {
            Map<String, Object> headersMap = (Map<String, Object>) ReflectionUtils
                    .getField(this.headersField, message.getHeaders());
            if (StringUtils.hasText(targetProtocol)) {
                headersMap.putIfAbsent(MessageUtils.TARGET_PROTOCOL, targetProtocol);
            }
            if (CloudEventMessageUtils.isCloudEvent(message)) {
                headersMap.putIfAbsent(MessageUtils.MESSAGE_TYPE, CloudEventMessageUtils.CLOUDEVENT_VALUE);
            }
            if (message != null && consumerProperties != null) {
                headersMap.put(FunctionProperties.SKIP_CONVERSION_HEADER, consumerProperties.isUseNativeDecoding());
            }
            Object result = function.apply(message);
            if (result instanceof Publisher && this.isRoutingFunction) {
                throw new IllegalStateException("Routing to functions that return Publisher "
                        + "is not supported in the context of Spring Cloud Stream.");
            }
            return result;
        }
    }
}

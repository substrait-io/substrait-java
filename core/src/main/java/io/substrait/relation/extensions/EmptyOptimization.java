package io.substrait.relation.extensions;

import io.substrait.extension.AdvancedExtension;

/**
 * Default type to which {@link AdvancedExtension#getOptimizations()} data is converted to by the
 * {@link io.substrait.extension.ProtoExtensionConverter}
 */
public class EmptyOptimization implements AdvancedExtension.Optimization {}

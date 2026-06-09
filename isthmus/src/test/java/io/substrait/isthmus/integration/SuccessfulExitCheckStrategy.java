package io.substrait.isthmus.integration;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.utility.DockerStatus;

/**
 * A {@link StartupCheckStrategy} that checks if the container has exited with a successful exit
 * code. This allows use of a container that is launched purely to execute a command prior to
 * startup of dependent containers.
 */
public class SuccessfulExitCheckStrategy extends StartupCheckStrategy {
  @Override
  public StartupStatus checkStartupState(DockerClient dockerClient, String containerId) {
    InspectContainerResponse.ContainerState state = getCurrentState(dockerClient, containerId);

    if (!DockerStatus.isContainerStopped(state)) {
      return StartupStatus.NOT_YET_KNOWN;
    }

    if (!DockerStatus.isContainerExitCodeSuccess(state)) {
      return StartupStatus.FAILED;
    }

    return StartupStatus.SUCCESSFUL;
  }
}

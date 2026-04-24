buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  buildNode = 'jenkins-agent-java21'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      // health check test located in org.folio.rest.OkapiHealthAPITest
    }
  }
}

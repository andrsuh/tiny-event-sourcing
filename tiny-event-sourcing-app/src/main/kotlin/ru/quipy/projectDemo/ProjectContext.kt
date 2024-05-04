package ru.quipy.projectDemo

import ru.quipy.TinyEsLibConfig
import ru.quipy.application.Component
import ru.quipy.projectDemo.config.ProjectDemoConfig
import ru.quipy.projectDemo.projections.AnnotationBasedProjectEventsSubscriber
import ru.quipy.projectDemo.projections.ProjectEventsSubscriber

class ProjectContext private constructor() {
    private lateinit var annotationBasedProjectEventsSubscriber: AnnotationBasedProjectEventsSubscriber
    private lateinit var components: List<Component>
    private lateinit var projectEventsSubscriber: ProjectEventsSubscriber
    private lateinit var projectDemoConfig: ProjectDemoConfig

    constructor(tinyEsLibConfig: TinyEsLibConfig) : this() {
        annotationBasedProjectEventsSubscriber = AnnotationBasedProjectEventsSubscriber()
        projectDemoConfig = projectDemoConfig(tinyEsLibConfig)
        projectEventsSubscriber = ProjectEventsSubscriber(tinyEsLibConfig.subscriptionsManager)
        components = mutableListOf(projectDemoConfig, annotationBasedProjectEventsSubscriber, projectEventsSubscriber)
        components.forEach { it.postConstruct() }
    }

    private fun projectDemoConfig(config: TinyEsLibConfig) : ProjectDemoConfig {
        return ProjectDemoConfig(
            config.subscriptionsManager,
            annotationBasedProjectEventsSubscriber,
            config.eventSourcingServiceFactory,
            config.eventStreamManager,
            config.aggregateRegistry
        )
    }
}
package ru.quipy.projectDemo

import ru.quipy.TinyEsLibConfig
import ru.quipy.application.component.Component
import ru.quipy.projectDemo.config.ProjectDemoConfig
import ru.quipy.projectDemo.projections.AnnotationBasedProjectEventsSubscriber
import ru.quipy.projectDemo.projections.ProjectEventsSubscriber

class ProjectContext private constructor() {
    lateinit var annotationBasedProjectEventsSubscriber: AnnotationBasedProjectEventsSubscriber
    lateinit var components: List<Component>
    lateinit var projectEventsSubscriber: ProjectEventsSubscriber
    lateinit var projectDemoConfig: ProjectDemoConfig

    constructor(tinyEsLibConfig: TinyEsLibConfig) : this() {
        annotationBasedProjectEventsSubscriber = AnnotationBasedProjectEventsSubscriber()
        projectDemoConfig = projectDemoConfig(tinyEsLibConfig)
        projectEventsSubscriber = ProjectEventsSubscriber(tinyEsLibConfig.subscriptionsManager)

        components = mutableListOf(projectDemoConfig, projectEventsSubscriber, annotationBasedProjectEventsSubscriber)
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
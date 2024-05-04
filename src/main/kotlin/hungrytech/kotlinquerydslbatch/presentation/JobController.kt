package hungrytech.kotlinquerydslbatch.presentation

import java.time.LocalDateTime
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RequestMapping("/api/jobs")
@RestController
class JobController(
    private val jobLauncher: JobLauncher,
    private val testJob: Job
) {

    @PostMapping
    fun startJob() {
        jobLauncher.run(testJob, JobParametersBuilder()
            .addLocalDateTime("currentTimeStamps", LocalDateTime.now())
            .addString("jobName", "testJob")
            .toJobParameters()
        )
    }
}

package org.tair;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication
public class PantherDataPipelineApplication extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(PantherDataPipelineApplication.class);
	}

	public static void main(String[] args) {
//		SpringApplication.run(PantherDataPipelineApplication.class, args);
		SpringApplication sa = new SpringApplication(PantherDataPipelineApplication.class);
		System.out.println("Running main");
		sa.run(args);
	}

	// @RestController
	// public static class MyController {

	// 	@RequestMapping("/")
	// 	public String handler (Model model) {
	// 		model.addAttribute("msg",
	// 				"a spring-boot war example");
	// 		return "myPage";
	// 	}
	// }
}

package com.synctask.controller;

import com.synctask.dto.ChangePasswordRequest;
import com.synctask.dto.JwtResponse;
import com.synctask.dto.LoginRequest;
import com.synctask.dto.RegisterRequest;
import com.synctask.entity.AuditLog;
import com.synctask.entity.User;
import com.synctask.repository.UserRepository;
import com.synctask.security.UserPrincipal;
import com.synctask.service.AuditLogService;
import com.synctask.service.AuthService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.*;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private static final Logger logger = LoggerFactory.getLogger(AuthController.class);

    @Autowired
    private AuthService authService;

    @Autowired
    private AuditLogService auditLogService;

    @Autowired
    private UserRepository userRepository;

    private String translateLoginError(Exception e) {
        if (e instanceof BadCredentialsException || e instanceof UsernameNotFoundException) {
            return "用户名或密码错误";
        } else if (e instanceof DisabledException) {
            return "账号已被禁用";
        } else if (e instanceof LockedException) {
            return "账号已被锁定";
        } else if (e instanceof AccountExpiredException) {
            return "账号已过期";
        } else if (e instanceof CredentialsExpiredException) {
            return "密码已过期";
        } else if (e instanceof AuthenticationException) {
            logger.warn("Login authentication error: {}", e.getMessage());
            return "认证失败，请稍后重试";
        } else {
            logger.error("Login unexpected error", e);
            return "登录失败，请稍后重试";
        }
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@Valid @RequestBody RegisterRequest request) {
        try {
            User user = authService.register(request);
            return ResponseEntity.ok(new ApiResponse(true, "注册成功", user));
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@Valid @RequestBody LoginRequest request) {
        try {
            JwtResponse response = authService.login(request);
            // 审计日志：登录成功
            Optional<User> userOpt = userRepository.findByUsername(request.getUsername());
            Long userId = userOpt.map(User::getId).orElse(null);
            Map<String, Object> details = new HashMap<>();
            details.put("username", request.getUsername());
            auditLogService.logSuccess(userId, AuditLog.Action.LOGIN, null, details);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // 审计日志：登录失败
            Optional<User> userOpt = userRepository.findByUsername(request.getUsername());
            Long userId = userOpt.map(User::getId).orElse(null);
            Map<String, Object> details = new HashMap<>();
            details.put("username", request.getUsername());
            auditLogService.logFailure(userId, AuditLog.Action.LOGIN, null, details, translateLoginError(e));
            return ResponseEntity.badRequest().body(new ApiResponse(false, translateLoginError(e)));
        }
    }

    @GetMapping("/me")
    public ResponseEntity<?> getCurrentUser(Authentication authentication) {
        try {
            User user = authService.getCurrentUser(authentication);
            return ResponseEntity.ok(new ApiResponse(true, "获取用户信息成功", user));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    @PostMapping("/change-password")
    public ResponseEntity<?> changePassword(
            @Valid @RequestBody ChangePasswordRequest request,
            Authentication authentication) {
        if (authentication == null || !(authentication.getPrincipal() instanceof UserPrincipal)) {
            return ResponseEntity.status(401).body(new ApiResponse(false, "未登录或登录已过期"));
        }
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        Map<String, Object> details = new HashMap<>();
        details.put("username", userPrincipal.getUsername());
        try {
            authService.changePassword(userPrincipal.getId(), request);
            auditLogService.logSuccess(userPrincipal.getId(), AuditLog.Action.CHANGE_PASSWORD, null, details);
            return ResponseEntity.ok(new ApiResponse(true, "密码修改成功"));
        } catch (Exception e) {
            auditLogService.logFailure(userPrincipal.getId(), AuditLog.Action.CHANGE_PASSWORD, null, details, e.getMessage());
            return ResponseEntity.badRequest().body(new ApiResponse(false, e.getMessage()));
        }
    }

    public static class ApiResponse {
        private boolean success;
        private String message;
        private Object data;

        public ApiResponse(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public ApiResponse(boolean success, String message, Object data) {
            this.success = success;
            this.message = message;
            this.data = data;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }
    }
}

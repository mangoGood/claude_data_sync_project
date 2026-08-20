package com.synctask.service;

import com.synctask.dto.ChangePasswordRequest;
import com.synctask.dto.JwtResponse;
import com.synctask.dto.LoginRequest;
import com.synctask.dto.RegisterRequest;
import com.synctask.entity.Role;
import com.synctask.entity.User;
import com.synctask.repository.UserRepository;
import com.synctask.security.JwtTokenProvider;
import com.synctask.security.UserPrincipal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuthService {
    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private JwtTokenProvider tokenProvider;

    /**
     * 建号。**接口本身只对 ADMIN 开放**（见 {@code SecurityConfig}）——此前
     * {@code /api/auth/**} 整段 permitAll，任何人都能自助注册出一个可读写全部同步任务、
     * 全部连接凭证、全部证书的账号。首个管理员由 Flyway 的 V2 种子建出，不走这个接口，
     * 因此这里不需要（也不应该有）"用户表为空就放行"的引导分支——那是个 TOCTOU 缺口。
     */
    @Transactional
    public User register(RegisterRequest request) {
        if (userRepository.existsByUsername(request.getUsername())) {
            throw new RuntimeException("用户名已被使用");
        }

        if (request.getEmail() != null && userRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("邮箱已被使用");
        }

        // 留空按最小可用权限（USER），非法值直接抛而不是静默降级——
        // 静默降级在建号场景下会得到一个"看起来建成了、权限却不是预期"的账号。
        String role = (request.getRole() == null || request.getRole().isBlank())
                ? Role.USER
                : Role.normalize(request.getRole());

        User user = new User();
        user.setUsername(request.getUsername());
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setEmail(request.getEmail());
        user.setRole(role);
        user.setEnabled(true);

        return userRepository.save(user);
    }

    public JwtResponse login(LoginRequest request) {
        Authentication authentication = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.getUsername(), request.getPassword())
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);
        String jwt = tokenProvider.generateToken(authentication);

        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();

        return new JwtResponse(jwt, userPrincipal.getId(), userPrincipal.getUsername(), 
                userPrincipal.getEmail(), userPrincipal.getRole());
    }

    public User getCurrentUser(Authentication authentication) {
        UserPrincipal userPrincipal = (UserPrincipal) authentication.getPrincipal();
        return userRepository.findById(userPrincipal.getId())
                .orElseThrow(() -> new RuntimeException("用户不存在"));
    }

    @Transactional
    public void changePassword(Long userId, ChangePasswordRequest request) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("用户不存在"));

        if (!passwordEncoder.matches(request.getOldPassword(), user.getPassword())) {
            throw new RuntimeException("原密码不正确");
        }

        user.setPassword(passwordEncoder.encode(request.getNewPassword()));
        // 递增令牌版本：签发过的所有旧 token（tv 不匹配）立即失效，改密后必须重新登录。
        user.setTokenVersion((user.getTokenVersion() != null ? user.getTokenVersion() : 0) + 1);
        userRepository.save(user);
    }
}
